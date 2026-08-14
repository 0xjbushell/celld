//! Optional Azure Blob conformance coverage. Set
//! `AZURE_STORAGE_USE_EMULATOR=1` (and start Azurite) or provide
//! `CELLD_AZURE_INTEGRATION_CONTAINER` plus the explicit account-key/SAS
//! environment required by celld's Azure backend.

#![cfg(feature = "azure")]

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use celld_ltx::client::object_store::{ObjectStoreClient, ObjectStoreConfig};
use celld_ltx::client::{make_test_ltx_file, run_client_suite, ReplicaClient};
use celld_ltx::ltx::{self, Header, HEADER_FLAG_NO_CHECKSUM, VERSION};
use celld_ltx::TXID;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::{path::Path, Attribute, GetOptions, ObjectStore};

enum AzureEnvironment {
    Emulator { container: String },
    Real { container: String },
}

struct AzureClient {
    client: ObjectStoreClient,
    store: Arc<dyn ObjectStore>,
    path: String,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn selected_azure_environment(
    get: impl Fn(&str) -> Option<String>,
) -> Result<Option<AzureEnvironment>, String> {
    if get("AZURE_STORAGE_USE_EMULATOR").as_deref() == Some("1") {
        return Ok(Some(AzureEnvironment::Emulator {
            container: "celld-test".to_string(),
        }));
    }
    Ok(get("CELLD_AZURE_INTEGRATION_CONTAINER")
        .map(|container| AzureEnvironment::Real { container }))
}

fn endpoint_addresses(endpoint: &str) -> Option<Vec<SocketAddr>> {
    let endpoint = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let hostport = endpoint.split('/').next().unwrap_or(endpoint);
    let (host, port) = hostport.rsplit_once(':').unwrap_or((hostport, "443"));
    let port = port.parse().ok()?;
    (host, port).to_socket_addrs().ok().map(Iterator::collect)
}

fn reachable(endpoint: &str) -> bool {
    endpoint_addresses(endpoint)
        .into_iter()
        .flatten()
        .any(|addr| std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(750)).is_ok())
}

fn require_reachable(
    endpoint: &str,
    is_reachable: impl FnOnce(&str) -> bool,
) -> Result<(), String> {
    if is_reachable(endpoint) {
        Ok(())
    } else {
        Err(format!(
            "Azure integration selected but endpoint is unavailable at {endpoint}"
        ))
    }
}

fn unique_path(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    format!("celld-azure-test/{tag}/{nanos}")
}

fn require_env(get: &impl Fn(&str) -> Option<String>, name: &str) -> Result<String, String> {
    get(name).ok_or_else(|| format!("{name} is required for selected Azure integration"))
}

fn parse_sas_authorization(sas: &str) -> Result<Vec<(String, String)>, String> {
    let sas = sas.strip_prefix('?').unwrap_or(sas);
    if sas.is_empty() {
        return Err("AZURE_STORAGE_SAS_KEY must contain query pairs".to_string());
    }
    sas.split('&')
        .map(|pair| {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| format!("invalid SAS query pair: {pair:?}"))?;
            if key.is_empty() {
                return Err("invalid SAS query pair with an empty key".to_string());
            }
            Ok((decode_sas_component(key)?, decode_sas_component(value)?))
        })
        .collect()
}

fn decode_sas_component(component: &str) -> Result<String, String> {
    let bytes = component.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            return Err("invalid percent encoding in SAS query pair".to_string());
        }
    }
    percent_encoding::percent_decode_str(component)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|error| format!("invalid UTF-8 in SAS query pair: {error}"))
}

fn configured_builder(
    environment: AzureEnvironment,
    get: impl Fn(&str) -> Option<String>,
) -> Result<(String, String, MicrosoftAzureBuilder), String> {
    match environment {
        AzureEnvironment::Emulator { container } => {
            let endpoint =
                get("AZURITE_BLOB_STORAGE_URL").unwrap_or_else(|| "http://127.0.0.1:10000".into());
            Ok((
                container.clone(),
                endpoint,
                MicrosoftAzureBuilder::new()
                    .with_container_name(container)
                    .with_use_emulator(true),
            ))
        }
        AzureEnvironment::Real { container } => {
            let account = require_env(&get, "AZURE_STORAGE_ACCOUNT_NAME")?;
            let endpoint = get("AZURE_STORAGE_ENDPOINT")
                .unwrap_or_else(|| format!("https://{account}.blob.core.windows.net"));
            let builder = MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(&container)
                .with_endpoint(endpoint.clone())
                .with_allow_http(true);
            let builder = match require_env(&get, "CELLD_AZURE_AUTH")?.as_str() {
                "account-key" => {
                    builder.with_access_key(require_env(&get, "AZURE_STORAGE_ACCOUNT_KEY")?)
                }
                "sas" => builder.with_sas_authorization(parse_sas_authorization(&require_env(
                    &get,
                    "AZURE_STORAGE_SAS_KEY",
                )?)?),
                mode => {
                    return Err(format!(
                        "CELLD_AZURE_AUTH={mode} is unsupported for selected Azure integration"
                    ))
                }
            };
            Ok((container, endpoint, builder))
        }
    }
}

fn make_client(path: String) -> Result<Option<AzureClient>, String> {
    let Some(environment) = selected_azure_environment(env)? else {
        return Ok(None);
    };
    let (container, endpoint, builder) = configured_builder(environment, env)?;
    require_reachable(&endpoint, reachable)?;
    let store = Arc::new(
        builder
            .build()
            .map_err(|error| format!("build configured Azure store: {error}"))?,
    );
    Ok(Some(AzureClient {
        client: ObjectStoreClient::with_store(
            ObjectStoreConfig {
                bucket: container,
                path: path.clone(),
                timestamp_metadata_key: "litestream_timestamp".into(),
                ..Default::default()
            },
            store.clone(),
        ),
        store,
        path,
    }))
}

#[test]
fn azure_integration_skips_only_without_an_environment_selection() {
    assert!(selected_azure_environment(|_| None).unwrap().is_none());
}

#[test]
fn selected_azure_integration_fails_for_incomplete_or_unreachable_configuration() {
    let environment = selected_azure_environment(|name| {
        (name == "CELLD_AZURE_INTEGRATION_CONTAINER").then(|| "container".to_string())
    })
    .unwrap()
    .expect("real integration environment is selected");
    assert!(configured_builder(environment, |_| None)
        .unwrap_err()
        .contains("AZURE_STORAGE_ACCOUNT_NAME"));
    assert!(require_reachable("http://127.0.0.1:1", |_| false)
        .unwrap_err()
        .contains("selected but endpoint is unavailable"));
}

fn ltx_key(path: &str, min_txid: TXID, max_txid: TXID) -> Path {
    Path::from(format!(
        "{path}/0000/{}",
        ltx::format_filename(min_txid, max_txid)
    ))
}

async fn assert_timestamp_metadata(azure: &AzureClient, min_txid: TXID, max_txid: TXID) {
    let result = azure
        .store
        .get_opts(
            &ltx_key(&azure.path, min_txid, max_txid),
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .expect("head Azure LTX object");
    assert_eq!(
        result
            .attributes
            .get(&Attribute::Metadata("litestream_timestamp".into()))
            .map(|value| value.as_ref()),
        Some("1970-01-01T00:00:00Z"),
        "Azure LTX metadata uses the Azure-safe timestamp key"
    );
}

fn large_ltx(min_bytes: usize) -> Vec<u8> {
    let page_size = 4096;
    let lock = ltx::lock_pgno(page_size);
    let mut pages = Vec::new();
    let mut pgno = 1;
    while pages.len() * (page_size as usize) < min_bytes {
        if pgno == lock {
            pgno += 1;
            continue;
        }
        let mut state = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(pgno as u64 + 1);
        let mut page = vec![0; page_size as usize];
        for byte in &mut page {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        pages.push((pgno, page));
        pgno += 1;
    }
    let header = Header {
        version: VERSION,
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size,
        commit: pages.last().expect("pages").0,
        min_txid: TXID(1),
        max_txid: TXID(1),
        timestamp: 0,
        pre_apply_checksum: 0,
        wal_offset: 0,
        wal_size: 0,
        wal_salt1: 0,
        wal_salt2: 0,
        node_id: 0,
    };
    ltx::encode_file(&header, &pages, 0).expect("encode multipart LTX")
}

#[tokio::test]
async fn azure_object_store_passes_conformance_and_cleanup() {
    let Some(azure) =
        make_client(unique_path("conformance")).expect("configure selected Azure integration")
    else {
        return;
    };
    run_client_suite(&azure.client).await;
    azure
        .client
        .delete_all()
        .await
        .expect("cleanup Azure conformance data");
}

#[tokio::test]
async fn azure_single_put_round_trips_exact_bytes_and_timestamp_metadata() {
    let Some(azure) =
        make_client(unique_path("single")).expect("configure selected Azure integration")
    else {
        return;
    };
    let data = make_test_ltx_file(TXID(1), TXID(1), 7);
    azure
        .client
        .write_ltx_file(0, TXID(1), TXID(1), &data)
        .await
        .expect("single Azure write");
    assert_eq!(
        azure
            .client
            .open_ltx_file(0, TXID(1), TXID(1))
            .await
            .expect("read Azure single object"),
        data
    );
    assert_timestamp_metadata(&azure, TXID(1), TXID(1)).await;
    azure
        .client
        .delete_all()
        .await
        .expect("cleanup Azure single data");
}

#[tokio::test]
async fn azure_multipart_round_trips_exact_bytes_and_timestamp_metadata() {
    let Some(azure) =
        make_client(unique_path("multipart")).expect("configure selected Azure integration")
    else {
        return;
    };
    let data = large_ltx(6 * 1024 * 1024);
    assert!(data.len() >= 5 * 1024 * 1024, "must use multipart upload");
    azure
        .client
        .write_ltx_file(0, TXID(1), TXID(1), &data)
        .await
        .expect("multipart Azure write");
    assert_eq!(
        azure
            .client
            .open_ltx_file(0, TXID(1), TXID(1))
            .await
            .expect("read Azure multipart object"),
        data
    );
    assert_timestamp_metadata(&azure, TXID(1), TXID(1)).await;
    azure
        .client
        .delete_all()
        .await
        .expect("cleanup Azure multipart data");
}

#[tokio::test]
async fn azure_bounded_seek_filters_retained_history() {
    let Some(azure) =
        make_client(unique_path("bounded-seek")).expect("configure selected Azure integration")
    else {
        return;
    };
    const HISTORY: u64 = 64;
    for txid in 1..=HISTORY {
        let data = make_test_ltx_file(TXID(txid), TXID(txid), txid as u8);
        azure
            .client
            .write_ltx_file(0, TXID(txid), TXID(txid), &data)
            .await
            .expect("write retained history");
    }
    let started = std::time::Instant::now();
    let files = azure
        .client
        .ltx_files_bounded(0, TXID(60), 3)
        .await
        .expect("bounded Azure seek");
    let txids: Vec<u64> = files.iter().map(|file| file.min_txid.0).collect();
    assert_eq!(txids, vec![60, 61, 62]);
    eprintln!(
        "Azure bounded seek retained-history fixture: {HISTORY} objects, {} ms scan",
        started.elapsed().as_millis()
    );
    azure
        .client
        .delete_all()
        .await
        .expect("cleanup Azure retained history");
}
