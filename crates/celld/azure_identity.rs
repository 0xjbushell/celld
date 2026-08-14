// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The official Azure SDK token adapter used by the Azure Blob object store.

use async_trait::async_trait;
use azure_core::credentials::{AccessToken, TokenCredential};
use azure_core::time::{Duration, OffsetDateTime};
use object_store::azure::AzureCredential;
use object_store::CredentialProvider;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

pub(crate) const AZURE_STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
const REFRESH_WINDOW: Duration = Duration::minutes(5);

trait Clock: fmt::Debug + Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

#[derive(Debug)]
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// Caches official Azure SDK access tokens before handing them to object-store.
pub(crate) struct AzureTokenCredentialProvider {
    credential: Arc<dyn TokenCredential>,
    cache: Mutex<Option<AccessToken>>,
    refresh: Mutex<Option<Arc<RefreshCohort>>>,
    clock: Arc<dyn Clock>,
}

#[derive(Clone, Debug)]
enum RefreshOutcome {
    Token(AccessToken),
    Error(Arc<azure_core::Error>),
}

impl RefreshOutcome {
    fn credential(self) -> object_store::Result<Arc<AzureCredential>> {
        match self {
            Self::Token(token) => Ok(AzureTokenCredentialProvider::bearer(token)),
            Self::Error(error) => Err(object_store::Error::Generic {
                store: "AzureIdentityCredential",
                source: Box::new(SharedSdkError(error)),
            }),
        }
    }
}

#[derive(Debug)]
struct SharedSdkError(Arc<azure_core::Error>);

impl fmt::Display for SharedSdkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl StdError for SharedSdkError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref())
    }
}

#[derive(Debug)]
struct RefreshCohort {
    completion: watch::Sender<Option<RefreshOutcome>>,
}

enum RefreshRole {
    Leader {
        cohort: Arc<RefreshCohort>,
        stale: Option<AccessToken>,
    },
    Waiter(watch::Receiver<Option<RefreshOutcome>>),
}

impl fmt::Debug for AzureTokenCredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AzureTokenCredentialProvider")
            .finish_non_exhaustive()
    }
}

impl AzureTokenCredentialProvider {
    pub(crate) fn new(credential: Arc<dyn TokenCredential>) -> Arc<Self> {
        Arc::new(Self {
            credential,
            cache: Mutex::new(None),
            refresh: Mutex::new(None),
            clock: Arc::new(SystemClock),
        })
    }

    fn is_fresh(token: &AccessToken, now: OffsetDateTime) -> bool {
        token.expires_on > now + REFRESH_WINDOW
    }

    fn bearer(token: AccessToken) -> Arc<AzureCredential> {
        Arc::new(AzureCredential::BearerToken(
            token.token.secret().to_string(),
        ))
    }
}

#[async_trait]
impl CredentialProvider for AzureTokenCredentialProvider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let now = self.clock.now();
        if let Some(token) = self
            .cache
            .lock()
            .await
            .clone()
            .filter(|token| Self::is_fresh(token, now))
        {
            return Ok(Self::bearer(token));
        }

        let role = {
            let mut refresh = self.refresh.lock().await;
            let now = self.clock.now();
            let stale = self.cache.lock().await.clone();
            if let Some(token) = stale.clone().filter(|token| Self::is_fresh(token, now)) {
                return Ok(Self::bearer(token));
            }

            match refresh.as_ref() {
                Some(cohort) => RefreshRole::Waiter(cohort.completion.subscribe()),
                None => {
                    let (completion, _) = watch::channel(None);
                    let cohort = Arc::new(RefreshCohort { completion });
                    *refresh = Some(cohort.clone());
                    RefreshRole::Leader { cohort, stale }
                }
            }
        };

        let RefreshRole::Leader { cohort, stale } = role else {
            let RefreshRole::Waiter(mut completion) = role else {
                unreachable!("refresh role is either leader or waiter");
            };
            if completion.borrow().is_none() {
                completion
                    .changed()
                    .await
                    .map_err(|_| object_store::Error::Generic {
                        store: "AzureIdentityCredential",
                        source: "Azure identity refresh cohort ended unexpectedly".into(),
                    })?;
            }
            return completion
                .borrow_and_update()
                .clone()
                .expect("completed refresh cohort has an outcome")
                .credential();
        };

        let outcome = match self
            .credential
            .get_token(&[AZURE_STORAGE_SCOPE], None)
            .await
        {
            Ok(token) => {
                *self.cache.lock().await = Some(token.clone());
                RefreshOutcome::Token(token)
            }
            Err(error) => {
                let failure_time = self.clock.now();
                if let Some(token) = stale.filter(|token| token.expires_on > failure_time) {
                    RefreshOutcome::Token(token)
                } else {
                    RefreshOutcome::Error(Arc::new(error))
                }
            }
        };
        let _ = cohort.completion.send(Some(outcome.clone()));
        let mut refresh = self.refresh.lock().await;
        if refresh
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &cohort))
        {
            *refresh = None;
        }
        drop(refresh);
        outcome.credential()
    }
}

#[cfg(test)]
mod tests {
    use super::{AzureTokenCredentialProvider, Clock, REFRESH_WINDOW};
    use async_trait::async_trait;
    use azure_core::credentials::{AccessToken, TokenCredential, TokenRequestOptions};
    use azure_core::error::ErrorKind;
    use azure_core::time::{Duration, OffsetDateTime};
    use object_store::azure::AzureCredential;
    use object_store::CredentialProvider;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug)]
    struct FixedClock(OffsetDateTime);

    impl Clock for FixedClock {
        fn now(&self) -> OffsetDateTime {
            self.0
        }
    }

    #[derive(Debug)]
    struct RefreshAdvancingClock {
        before_refresh: OffsetDateTime,
        after_refresh: OffsetDateTime,
        calls: AtomicUsize,
    }

    impl Clock for RefreshAdvancingClock {
        fn now(&self) -> OffsetDateTime {
            if self.calls.fetch_add(1, Ordering::SeqCst) < 2 {
                self.before_refresh
            } else {
                self.after_refresh
            }
        }
    }

    #[derive(Debug)]
    enum Outcome {
        Token(&'static str, OffsetDateTime),
        Error,
    }

    #[derive(Debug)]
    struct FakeCredential {
        outcomes: Mutex<VecDeque<Outcome>>,
        requests: AtomicUsize,
        scopes: Mutex<Vec<Vec<String>>>,
        delay: std::time::Duration,
    }

    impl FakeCredential {
        fn new(outcomes: impl IntoIterator<Item = Outcome>) -> Arc<Self> {
            Arc::new(Self {
                outcomes: Mutex::new(outcomes.into_iter().collect()),
                requests: AtomicUsize::new(0),
                scopes: Mutex::new(Vec::new()),
                delay: std::time::Duration::ZERO,
            })
        }

        fn with_delay(mut self: Arc<Self>, delay: std::time::Duration) -> Arc<Self> {
            Arc::get_mut(&mut self).expect("not shared yet").delay = delay;
            self
        }
    }

    #[async_trait]
    impl TokenCredential for FakeCredential {
        async fn get_token(
            &self,
            scopes: &[&str],
            _: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.scopes
                .lock()
                .await
                .push(scopes.iter().map(ToString::to_string).collect());
            tokio::time::sleep(self.delay).await;
            match self.outcomes.lock().await.pop_front() {
                Some(Outcome::Token(token, expires_on)) => Ok(AccessToken::new(token, expires_on)),
                Some(Outcome::Error) => Err(azure_core::Error::with_message(
                    ErrorKind::Credential,
                    "fake credential error".to_string(),
                )),
                None => Err(azure_core::Error::with_message(
                    ErrorKind::Credential,
                    "unexpected token request".to_string(),
                )),
            }
        }
    }

    fn provider(
        credential: Arc<FakeCredential>,
        now: OffsetDateTime,
        cached: Option<AccessToken>,
    ) -> Arc<AzureTokenCredentialProvider> {
        Arc::new(AzureTokenCredentialProvider {
            credential,
            cache: Mutex::new(cached),
            refresh: Mutex::new(None),
            clock: Arc::new(FixedClock(now)),
        })
    }

    fn bearer(credential: Arc<AzureCredential>) -> String {
        match credential.as_ref() {
            AzureCredential::BearerToken(token) => token.clone(),
            other => panic!("expected bearer token, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn requests_the_exact_storage_scope_and_converts_to_a_bearer_token() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let credential =
            FakeCredential::new([Outcome::Token("bearer-token", now + Duration::hours(1))]);
        let provider = provider(credential.clone(), now, None);

        assert_eq!(
            bearer(provider.get_credential().await.expect("credential")),
            "bearer-token"
        );
        assert_eq!(
            *credential.scopes.lock().await,
            vec![vec!["https://storage.azure.com/.default".to_string()]]
        );
    }

    #[tokio::test]
    async fn reuses_a_token_before_the_five_minute_refresh_window() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let credential = FakeCredential::new([]);
        let provider = provider(
            credential.clone(),
            now,
            Some(AccessToken::new(
                "cached",
                now + REFRESH_WINDOW + Duration::seconds(1),
            )),
        );

        assert_eq!(
            bearer(provider.get_credential().await.expect("cached credential")),
            "cached"
        );
        assert_eq!(credential.requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn refreshes_at_the_five_minute_window() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let credential =
            FakeCredential::new([Outcome::Token("refreshed", now + Duration::hours(1))]);
        let provider = provider(
            credential.clone(),
            now,
            Some(AccessToken::new("old", now + REFRESH_WINDOW)),
        );

        assert_eq!(
            bearer(
                provider
                    .get_credential()
                    .await
                    .expect("refreshed credential")
            ),
            "refreshed"
        );
        assert_eq!(credential.requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_expired_token_callers_share_one_refresh_and_recheck_the_cache() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let credential =
            FakeCredential::new([Outcome::Token("refreshed", now + Duration::hours(1))])
                .with_delay(std::time::Duration::from_millis(20));
        let provider = provider(
            credential.clone(),
            now,
            Some(AccessToken::new("expired", now - Duration::seconds(1))),
        );

        let mut callers = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let provider = provider.clone();
            callers
                .spawn(async move { bearer(provider.get_credential().await.expect("credential")) });
        }
        while let Some(result) = callers.join_next().await {
            assert_eq!(result.expect("caller task"), "refreshed");
        }
        assert_eq!(credential.requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_expired_token_callers_share_a_failed_refresh_then_a_later_call_retries() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let credential = FakeCredential::new([
            Outcome::Error,
            Outcome::Token("retried", now + Duration::hours(1)),
        ])
        .with_delay(std::time::Duration::from_millis(20));
        let provider = provider(
            credential.clone(),
            now,
            Some(AccessToken::new("expired", now - Duration::seconds(1))),
        );

        let mut callers = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let provider = provider.clone();
            callers.spawn(async move {
                provider
                    .get_credential()
                    .await
                    .expect_err("the shared failed refresh must propagate")
                    .to_string()
            });
        }
        while let Some(result) = callers.join_next().await {
            assert!(result
                .expect("caller task")
                .contains("fake credential error"));
        }
        assert_eq!(credential.requests.load(Ordering::SeqCst), 1);

        assert_eq!(
            bearer(
                provider
                    .get_credential()
                    .await
                    .expect("later retry succeeds")
            ),
            "retried"
        );
        assert_eq!(credential.requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_refresh_uses_a_still_valid_cached_token() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let credential = FakeCredential::new([Outcome::Error]);
        let provider = provider(
            credential,
            now,
            Some(AccessToken::new("still-valid", now + Duration::minutes(1))),
        );

        assert_eq!(
            bearer(
                provider
                    .get_credential()
                    .await
                    .expect("valid cached credential")
            ),
            "still-valid"
        );
    }

    #[tokio::test]
    async fn failed_refresh_propagates_when_the_cached_token_expires_during_refresh() {
        let before_refresh = OffsetDateTime::UNIX_EPOCH;
        let after_refresh = before_refresh + Duration::seconds(2);
        let credential = FakeCredential::new([Outcome::Error]);
        let provider = Arc::new(AzureTokenCredentialProvider {
            credential,
            cache: Mutex::new(Some(AccessToken::new(
                "expires-during-refresh",
                before_refresh + Duration::seconds(1),
            ))),
            refresh: Mutex::new(None),
            clock: Arc::new(RefreshAdvancingClock {
                before_refresh,
                after_refresh,
                calls: AtomicUsize::new(0),
            }),
        });

        let error = provider
            .get_credential()
            .await
            .expect_err("expired token cannot authorize");
        assert!(error.to_string().contains("fake credential error"));
    }

    #[tokio::test]
    async fn failed_refresh_propagates_when_the_cached_token_expires_at_failure_time() {
        let before_refresh = OffsetDateTime::UNIX_EPOCH;
        let failure_time = before_refresh + Duration::seconds(1);
        let credential = FakeCredential::new([Outcome::Error]);
        let provider = Arc::new(AzureTokenCredentialProvider {
            credential,
            cache: Mutex::new(Some(AccessToken::new("expires-at-failure", failure_time))),
            refresh: Mutex::new(None),
            clock: Arc::new(RefreshAdvancingClock {
                before_refresh,
                after_refresh: failure_time,
                calls: AtomicUsize::new(0),
            }),
        });

        let error = provider
            .get_credential()
            .await
            .expect_err("a token expiring at failure time cannot authorize");
        assert!(error.to_string().contains("fake credential error"));
    }

    #[tokio::test]
    async fn failed_refresh_after_expiry_propagates_the_sdk_error() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let credential = FakeCredential::new([Outcome::Error]);
        let provider = provider(
            credential,
            now,
            Some(AccessToken::new("expired", now - Duration::seconds(1))),
        );

        let error = provider
            .get_credential()
            .await
            .expect_err("expired token cannot authorize");
        assert!(error.to_string().contains("fake credential error"));
    }
}
