//! `WriteBufferManager` is for managing memory allocation for one or more
//! MemTables.
use std::ptr::NonNull;
use std::sync::Arc;

use crate::{ffi, Cache};

pub(crate) struct WriteBufferManagerWrapper {
    pub(crate) inner: NonNull<ffi::rocksdb_write_buffer_manager_t>,
}

// Just like the types in the `db_options` module, these are only safe because
// the underlying types are thread-safe. This thread-safety is not actually
// documented, but an evaluation of the code revealed that all member
// variables are protected by mutexes or atomics. Also, sharing `WriteBufferManager`'s
// across threads is the intended usecase for the underlying cpp type.
unsafe impl Send for WriteBufferManagerWrapper {}
unsafe impl Sync for WriteBufferManagerWrapper {}

impl Drop for WriteBufferManagerWrapper {
    // Safety: `inner` is guaranteed to point to a `shared_ptr` to the
    // underlying cpp `WriteBufferManager`.
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_write_buffer_manager_destroy(self.inner.as_ptr());
        }
    }
}

/// A `WriteBufferManager`, which can be `Cloned` and shared across RocksDB instances to control
/// global memory usage. See
/// <https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager>
/// for more information.
// Note that we _could_ clone the underlying `std::shared_ptr`, but just storing
// it in an `Arc`, with the underlying calls (like `rocksdb_options_set_write_buffer_manager`)
// copy-construct the underlying `std::shared_ptr` is much easier, and is the same way
// we do it for types like `db_options::Cache`.
#[derive(Clone)]
pub struct WriteBufferManager(pub(crate) Arc<WriteBufferManagerWrapper>);

impl WriteBufferManager {
    /// Creates a new `WriteBufferManager` with a set `buffer_size`.
    ///
    /// buffer_size = 0 indicates no limit.
    pub fn new(buffer_size: usize) -> WriteBufferManager {
        Self::new_with_allow_stall(buffer_size, false)
    }

    /// Creates a new `WriteBufferManager` with a set `buffer_size`, and an `allow_stall`
    /// configuration.
    ///
    /// allow_stall: if set true, it will enable stalling of writes when
    /// memory_usage() exceeds buffer_size. It will wait for flush to complete and
    /// memory usage to drop down.
    ///
    /// buffer_size = 0 indicates no limit.
    pub fn new_with_allow_stall(buffer_size: usize, allow_stall: bool) -> WriteBufferManager {
        WriteBufferManager(Arc::new(WriteBufferManagerWrapper {
            // Safety: `rocksdb_write_buffer_manager_create` is guaranteed to create a non-null and valid
            // pointer to the underlying cpp type.
            inner: NonNull::new(unsafe {
                ffi::rocksdb_write_buffer_manager_create(buffer_size, allow_stall)
            })
            .unwrap(),
        }))
    }

    /// Creates a new `WriteBufferManager` with a `Cache`.
    ///
    /// buffer_size: buffer_size = 0 indicates no limit.
    /// cache: RocksDB will put dummy entries in the cache and
    /// cost the memory allocated to the cache. It can be used even if _buffer_size
    /// = 0. Note that `Cache` can also be shared across RocksDB instances.
    /// See
    /// <https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager#cost-memory-used-in-memtable-to-block-cache>
    /// for more information.
    ///
    /// allow_stall: if set true, it will enable stalling of writes when
    /// memory_usage() exceeds buffer_size. It will wait for flush to complete and
    /// memory usage to drop down.
    pub fn new_with_cache(
        buffer_size: usize,
        cache: &Cache,
        allow_stall: bool,
    ) -> WriteBufferManager {
        WriteBufferManager(Arc::new(WriteBufferManagerWrapper {
            // Safety: `rocksdb_write_buffer_manager_create` is guaranteed to create a non-null and valid
            // pointer to the underlying cpp type.
            inner: NonNull::new(unsafe {
                ffi::rocksdb_write_buffer_manager_create_with_cache(
                    buffer_size,
                    cache.0.inner.as_ptr(),
                    allow_stall,
                )
            })
            .unwrap(),
        }))
    }

    /// Returns true if buffer_limit is passed to limit the total memory usage and
    /// is greater than 0.
    pub fn enabled(&self) -> bool {
        // Safety: `inner` is guaranteed to point to a `shared_ptr` to the
        // underlying cpp `WriteBufferManager`.
        unsafe { ffi::rocksdb_write_buffer_manager_enabled(self.0.inner.as_ptr()) != 0 }
    }

    /// Returns the total memory used by memtables if enabled.
    pub fn memory_usage(&self) -> Option<usize> {
        if self.enabled() {
            // Safety: `inner` is guaranteed to point to a `shared_ptr` to the
            // underlying cpp `WriteBufferManager`.
            Some(unsafe { ffi::rocksdb_write_buffer_manager_memory_usage(self.0.inner.as_ptr()) })
        } else {
            None
        }
    }

    /// Returns the buffer_size.
    pub fn buffer_size(&self) -> usize {
        // Safety: `inner` is guaranteed to point to a `shared_ptr` to the
        // underlying cpp `WriteBufferManager`.
        unsafe { ffi::rocksdb_write_buffer_manager_buffer_size(self.0.inner.as_ptr()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Options, DB};
    use std::iter;
    use tempfile::TempDir;

    #[test]
    fn write_buffer_manager_of_2db() {
        let tmp_dir1 = TempDir::new().unwrap();
        let tmp_dir2 = TempDir::new().unwrap();
        let cache = Cache::new_lru_cache(10240);
        let manager = WriteBufferManager::new_with_cache(102400, &cache, false);
        let mut op1 = Options::default();
        op1.create_if_missing(true);
        op1.set_write_buffer_manager(&manager);
        let mut op2 = Options::default();
        op2.create_if_missing(true);
        op2.set_write_buffer_manager(&manager);
        assert_eq!(manager.memory_usage(), Some(0));
        let db1 = DB::open(&op1, &tmp_dir1).unwrap();

        let mem1 = manager.memory_usage().unwrap();

        let db2 = DB::open(&op2, &tmp_dir2).unwrap();

        assert_eq!(manager.enabled(), true);
        let mem2 = manager.memory_usage().unwrap();
        assert!(mem2 > mem1);

        for i in 0..100 {
            let key = format!("k{}", i);
            let val = format!("v{}", i * i);
            let value: String = iter::repeat(val).take(i * i).collect::<Vec<_>>().concat();

            db1.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let mem3 = manager.memory_usage().unwrap();
        assert!(mem3 > mem2);

        for i in 0..100 {
            let key = format!("k{}", i);
            let val = format!("v{}", i * i);
            let value: String = iter::repeat(val).take(i * i).collect::<Vec<_>>().concat();

            db2.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let mem4 = manager.memory_usage().unwrap();
        assert!(mem4 > mem3);

        assert!(db2.flush().is_ok());
        let mem5 = manager.memory_usage().unwrap();
        assert!(mem5 < mem4);

        drop(db1);
        drop(db2);
        assert_eq!(manager.memory_usage(), Some(0));
    }

    #[test]
    fn write_buffer_manager_plain_new() {
        let tmp_dir1 = TempDir::new().unwrap();
        let manager = WriteBufferManager::new(102400);
        let mut op1 = Options::default();
        op1.create_if_missing(true);
        op1.set_write_buffer_manager(&manager);

        assert_eq!(manager.memory_usage(), Some(0));
        let db1 = DB::open(&op1, &tmp_dir1).unwrap();

        assert_eq!(manager.enabled(), true);
        assert!(manager.memory_usage().unwrap() > 0);

        for i in 0..100 {
            let key = format!("k{}", i);
            let val = format!("v{}", i * i);
            let value: String = iter::repeat(val).take(i * i).collect::<Vec<_>>().concat();

            db1.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        drop(db1);
        assert_eq!(manager.memory_usage(), Some(0));
    }
}
