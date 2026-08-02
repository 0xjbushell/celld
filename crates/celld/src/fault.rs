//! Test-only SQLite fault injection: a wrapper VFS that delegates to the
//! process default VFS but fails writes with `SQLITE_IOERR` while the current
//! thread has armed the fault. Compiled only into test builds —
//! production binaries contain none of this code and no seam for it.
use rusqlite::ffi;
use std::cell::Cell;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;
use std::mem::size_of;
use std::sync::Once;

const FAULT_VFS_NAME: &std::ffi::CStr = c"cells-fault-vfs";

thread_local! {
    static WRITE_FAULT: Cell<bool> = const { Cell::new(false) };
}

pub fn set_write_fault(enabled: bool) {
    WRITE_FAULT.with(|flag| flag.set(enabled));
}

fn write_fault_armed() -> bool {
    WRITE_FAULT.try_with(Cell::get).unwrap_or(false)
}

/// The shim's per-file state: SQLite allocates `szOsFile` bytes, we use the
/// front for this header and hand the tail to the real VFS as its file.
#[repr(C)]
struct ShimFile {
    base: ffi::sqlite3_file,
    real: *mut ffi::sqlite3_file,
}

unsafe fn real_file(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    (*file.cast::<ShimFile>()).real
}

unsafe fn real_vfs(vfs: *mut ffi::sqlite3_vfs) -> *mut ffi::sqlite3_vfs {
    (*vfs).pAppData.cast()
}

macro_rules! file_delegate {
    ($file:ident, $method:ident $(, $arg:expr)*) => {{
        let real = real_file($file);
        ((*(*real).pMethods).$method.expect("real io method"))(real $(, $arg)*)
    }};
}

macro_rules! vfs_delegate {
    ($vfs:ident, $method:ident $(, $arg:expr)*) => {{
        let real = real_vfs($vfs);
        ((*real).$method.expect("real vfs method"))(real $(, $arg)*)
    }};
}

unsafe extern "C" fn shim_close(file: *mut ffi::sqlite3_file) -> c_int {
    file_delegate!(file, xClose)
}
unsafe extern "C" fn shim_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    offset: i64,
) -> c_int {
    file_delegate!(file, xRead, buf, amt, offset)
}
unsafe extern "C" fn shim_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    offset: i64,
) -> c_int {
    if write_fault_armed() {
        return ffi::SQLITE_IOERR;
    }
    file_delegate!(file, xWrite, buf, amt, offset)
}
unsafe extern "C" fn shim_truncate(
    file: *mut ffi::sqlite3_file,
    size: i64,
) -> c_int {
    file_delegate!(file, xTruncate, size)
}
unsafe extern "C" fn shim_sync(
    file: *mut ffi::sqlite3_file,
    flags: c_int,
) -> c_int {
    file_delegate!(file, xSync, flags)
}
unsafe extern "C" fn shim_file_size(
    file: *mut ffi::sqlite3_file,
    size: *mut i64,
) -> c_int {
    file_delegate!(file, xFileSize, size)
}
unsafe extern "C" fn shim_lock(
    file: *mut ffi::sqlite3_file,
    level: c_int,
) -> c_int {
    file_delegate!(file, xLock, level)
}
unsafe extern "C" fn shim_unlock(
    file: *mut ffi::sqlite3_file,
    level: c_int,
) -> c_int {
    file_delegate!(file, xUnlock, level)
}
unsafe extern "C" fn shim_check_reserved_lock(
    file: *mut ffi::sqlite3_file,
    out: *mut c_int,
) -> c_int {
    file_delegate!(file, xCheckReservedLock, out)
}
unsafe extern "C" fn shim_file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    arg: *mut c_void,
) -> c_int {
    file_delegate!(file, xFileControl, op, arg)
}
unsafe extern "C" fn shim_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    file_delegate!(file, xSectorSize)
}
unsafe extern "C" fn shim_device_characteristics(
    file: *mut ffi::sqlite3_file,
) -> c_int {
    file_delegate!(file, xDeviceCharacteristics)
}
unsafe extern "C" fn shim_shm_map(
    file: *mut ffi::sqlite3_file,
    region: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    file_delegate!(file, xShmMap, region, size, extend, out)
}
unsafe extern "C" fn shim_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    count: c_int,
    flags: c_int,
) -> c_int {
    file_delegate!(file, xShmLock, offset, count, flags)
}
unsafe extern "C" fn shim_shm_barrier(file: *mut ffi::sqlite3_file) {
    file_delegate!(file, xShmBarrier)
}
unsafe extern "C" fn shim_shm_unmap(
    file: *mut ffi::sqlite3_file,
    delete_flag: c_int,
) -> c_int {
    file_delegate!(file, xShmUnmap, delete_flag)
}
unsafe extern "C" fn shim_fetch(
    file: *mut ffi::sqlite3_file,
    offset: i64,
    amt: c_int,
    out: *mut *mut c_void,
) -> c_int {
    file_delegate!(file, xFetch, offset, amt, out)
}
unsafe extern "C" fn shim_unfetch(
    file: *mut ffi::sqlite3_file,
    offset: i64,
    page: *mut c_void,
) -> c_int {
    file_delegate!(file, xUnfetch, offset, page)
}

static SHIM_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(shim_close),
    xRead: Some(shim_read),
    xWrite: Some(shim_write),
    xTruncate: Some(shim_truncate),
    xSync: Some(shim_sync),
    xFileSize: Some(shim_file_size),
    xLock: Some(shim_lock),
    xUnlock: Some(shim_unlock),
    xCheckReservedLock: Some(shim_check_reserved_lock),
    xFileControl: Some(shim_file_control),
    xSectorSize: Some(shim_sector_size),
    xDeviceCharacteristics: Some(shim_device_characteristics),
    xShmMap: Some(shim_shm_map),
    xShmLock: Some(shim_shm_lock),
    xShmBarrier: Some(shim_shm_barrier),
    xShmUnmap: Some(shim_shm_unmap),
    xFetch: Some(shim_fetch),
    xUnfetch: Some(shim_unfetch),
};

unsafe extern "C" fn shim_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let real = real_vfs(vfs);
    let shim = file.cast::<ShimFile>();
    let inner = file.cast::<u8>().add(size_of::<ShimFile>()).cast();
    (*shim).base.pMethods = std::ptr::null();
    (*shim).real = inner;
    let open = (*real).xOpen.expect("real vfs xOpen");
    let rc = open(real, name, inner, flags, out_flags);
    if !(*inner).pMethods.is_null() {
        (*shim).base.pMethods = &SHIM_IO_METHODS;
    }
    rc
}
unsafe extern "C" fn shim_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    vfs_delegate!(vfs, xDelete, name, sync_dir)
}
unsafe extern "C" fn shim_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    vfs_delegate!(vfs, xAccess, name, flags, out)
}
unsafe extern "C" fn shim_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    length: c_int,
    out: *mut c_char,
) -> c_int {
    vfs_delegate!(vfs, xFullPathname, name, length, out)
}
unsafe extern "C" fn shim_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    length: c_int,
    out: *mut c_char,
) -> c_int {
    vfs_delegate!(vfs, xRandomness, length, out)
}
unsafe extern "C" fn shim_sleep(
    vfs: *mut ffi::sqlite3_vfs,
    microseconds: c_int,
) -> c_int {
    vfs_delegate!(vfs, xSleep, microseconds)
}
unsafe extern "C" fn shim_current_time(
    vfs: *mut ffi::sqlite3_vfs,
    out: *mut f64,
) -> c_int {
    vfs_delegate!(vfs, xCurrentTime, out)
}
unsafe extern "C" fn shim_get_last_error(
    vfs: *mut ffi::sqlite3_vfs,
    length: c_int,
    out: *mut c_char,
) -> c_int {
    vfs_delegate!(vfs, xGetLastError, length, out)
}
unsafe extern "C" fn shim_current_time_int64(
    vfs: *mut ffi::sqlite3_vfs,
    out: *mut i64,
) -> c_int {
    vfs_delegate!(vfs, xCurrentTimeInt64, out)
}

fn install() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        // SAFETY: the default VFS outlives the process; the shim is leaked so
        // SQLite may hold it forever. Registration is not made the default,
        // so only connections opened by name route through the shim.
        unsafe {
            let real = ffi::sqlite3_vfs_find(std::ptr::null());
            assert!(!real.is_null(), "no default SQLite VFS");
            let vfs = Box::leak(Box::new(ffi::sqlite3_vfs {
                iVersion: 2,
                szOsFile: (*real).szOsFile + size_of::<ShimFile>() as c_int,
                mxPathname: (*real).mxPathname,
                pNext: std::ptr::null_mut(),
                zName: FAULT_VFS_NAME.as_ptr(),
                pAppData: real.cast(),
                xOpen: Some(shim_open),
                xDelete: Some(shim_delete),
                xAccess: Some(shim_access),
                xFullPathname: Some(shim_full_pathname),
                xDlOpen: None,
                xDlError: None,
                xDlSym: None,
                xDlClose: None,
                xRandomness: Some(shim_randomness),
                xSleep: Some(shim_sleep),
                xCurrentTime: Some(shim_current_time),
                xGetLastError: Some(shim_get_last_error),
                xCurrentTimeInt64: Some(shim_current_time_int64),
                xSetSystemCall: None,
                xGetSystemCall: None,
                xNextSystemCall: None,
            }));
            assert_eq!(ffi::sqlite3_vfs_register(vfs, 0), ffi::SQLITE_OK);
        }
    });
}

/// Open `path` through the fault-injection VFS.
pub fn open_database(path: &str) -> rusqlite::Result<rusqlite::Connection> {
    install();
    rusqlite::Connection::open_with_flags_and_vfs(
        path,
        rusqlite::OpenFlags::default(),
        FAULT_VFS_NAME.to_str().unwrap(),
    )
}
