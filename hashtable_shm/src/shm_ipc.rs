use libc::PTHREAD_PROCESS_SHARED;
use rustix::fs::{ftruncate, Mode};
use rustix::mm::{mmap, MapFlags, ProtFlags};
use rustix::shm;
use std::mem::size_of;
use std::mem::MaybeUninit;
use std::ptr::null_mut;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("RustixIo: {0}")]
    RustixIo(#[from] rustix::io::Errno),

    #[error("Mutex init: {0}")]
    MutexInit(String),

    #[error("Cond init: {0}")]
    CondInit(String),

    #[error("Buffer is full")]
    BufferFull,
}

pub struct ShmQueue<K: Clone, V: Clone> {
    buffer: SharedBuffer<K, V>,
    server: bool,
    name: String,
}

const BUFFER_SIZE: usize = 10;

/// Abstraction of the buffer in shared memory
///
/// Implements functions to mutate the data in the buffer synchronized via locks.
/// Ideally we would try to use UnsafeCell instead of a mutable reference if possible.
struct SharedBuffer<K: Clone, V: Clone>(*mut SharedBufferInner<K, V>);

#[repr(C)]
/// Representation of buffer allocated in shared memory
struct SharedBufferInner<K: Clone, V: Clone> {
    pub request_buffer: RingBuffer<Request<K, V>>,
    pub response_buffer: RingBuffer<Response<K, V>>,
}

#[repr(C)]
/// Ring buffer structure with locking
struct RingBuffer<T> {
    lock: libc::pthread_mutex_t,
    has_data: libc::pthread_cond_t,
    buffer: [T; BUFFER_SIZE],
    read_pos: usize,
    write_pos: usize,
}

#[repr(C)]
#[derive(Clone, PartialEq, Debug)]
/// Operations supported by the HashTable
pub enum Operation {
    Read,
    Insert,
    Delete,
}

#[repr(C)]
#[derive(Clone, Debug)]
/// Response sent to the client
///
/// `val` should be `0` for `Delete` operations and on failure for `Read`
pub struct Response<K: Clone, V: Clone> {
    pub operation: Operation,
    pub error: bool,
    pub key: K,
    pub val: V,
    pub counter: usize,
}

#[repr(C)]
#[derive(Clone, Debug)]
/// Request sent by the client
pub struct Request<K: Clone, V: Clone> {
    pub operation: Operation,
    pub key: K,
    pub val: V,
    pub counter: usize,
}

impl<K: Clone, V: Clone> ShmQueue<K, V> {
    pub fn new(name: &str, server: bool) -> Result<Self, Error> {
        let flags = match server {
            true => shm::OFlags::CREATE | shm::OFlags::EXCL | shm::OFlags::RDWR,
            false => shm::OFlags::EXCL | shm::OFlags::RDWR,
        };

        let fd = shm::open(name, flags, Mode::RUSR | Mode::WUSR)?;

        if server {
            ftruncate(&fd, size_of::<SharedBufferInner<K, V>>() as u64)?;
        }

        let buffer_ptr = unsafe {
            mmap(
                null_mut(),
                size_of::<SharedBufferInner<K, V>>(),
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                &fd,
                0,
            )?
        } as *mut SharedBufferInner<K, V>;
        let buffer = SharedBuffer(buffer_ptr);

        if server {
            buffer.init()?;
        }

        Ok(ShmQueue {
            buffer,
            server,
            name: name.to_string(),
        })
    }

    pub fn request_put(&self, request: &Request<K, V>) -> Result<(), Error> {
        self.buffer.request_put(request)
    }

    pub fn request_get(&self) -> Result<Request<K, V>, Error> {
        self.buffer.request_get()
    }

    pub fn response_put(&self, response: &Response<K, V>) -> Result<(), Error> {
        self.buffer.response_put(response)
    }

    pub fn response_get(&self) -> Result<Response<K, V>, Error> {
        self.buffer.response_get()
    }

    pub fn stop(&self) -> Result<(), Error> {
        if self.server {
            shm::unlink(&self.name)?
        }

        Ok(())
    }
}

// Explicitly implement Send and Sync for our SharedBuffer as we implement the locking ourself where necessary.
unsafe impl<K: Clone, V: Clone> Send for SharedBuffer<K, V> {}
unsafe impl<K: Clone, V: Clone> Sync for SharedBuffer<K, V> {}

// Note: all operations that mutate the memory are synchronized and the wrapped reference is assumed to be valid
impl<K: Clone, V: Clone> SharedBuffer<K, V> {
    pub fn request_put(&self, request: &Request<K, V>) -> Result<(), Error> {
        let request_buffer = unsafe { &mut (*(self.0)).request_buffer };
        request_buffer.put(request)
    }

    pub fn request_get(&self) -> Result<Request<K, V>, Error> {
        let request_buffer = unsafe { &mut (*(self.0)).request_buffer };
        request_buffer.get()
    }

    pub fn response_put(&self, response: &Response<K, V>) -> Result<(), Error> {
        let response_buffer = unsafe { &mut (*(self.0)).response_buffer };
        response_buffer.put(response)
    }

    pub fn response_get(&self) -> Result<Response<K, V>, Error> {
        let response_buffer = unsafe { &mut (*(self.0)).response_buffer };
        response_buffer.get()
    }

    pub fn init(&self) -> Result<(), Error> {
        let request_buffer = unsafe { &mut (*(self.0)).request_buffer };
        let response_buffer = unsafe { &mut (*(self.0)).response_buffer };

        request_buffer.init()?;
        response_buffer.init()?;

        Ok(())
    }
}

/// Initializes lock and configures it to be shareable between processes
fn setup_lock(lock: &mut libc::pthread_mutex_t) -> Result<(), Error> {
    let mut lock_attr = MaybeUninit::<libc::pthread_mutexattr_t>::uninit();

    unsafe {
        if libc::pthread_mutexattr_init(lock_attr.as_mut_ptr()) != 0 {
            return Err(Error::MutexInit("Failed creating attrs".to_string()));
        }

        if libc::pthread_mutexattr_setpshared(lock_attr.as_mut_ptr(), PTHREAD_PROCESS_SHARED) != 0 {
            return Err(Error::MutexInit(
                "Failed to set shared process attr".to_string(),
            ));
        }

        if libc::pthread_mutex_init(lock, lock_attr.as_mut_ptr()) != 0 {
            return Err(Error::MutexInit("Failed to init mutex".to_string()));
        }
    }

    Ok(())
}

/// Initializes condition and configures it to be shareable between processes
fn setup_cond(cond: &mut libc::pthread_cond_t) -> Result<(), Error> {
    let mut cond_attr = MaybeUninit::<libc::pthread_condattr_t>::uninit();

    unsafe {
        if libc::pthread_condattr_init(cond_attr.as_mut_ptr()) != 0 {
            return Err(Error::CondInit("Failed creating attrs".to_string()));
        }

        if libc::pthread_condattr_setpshared(cond_attr.as_mut_ptr(), PTHREAD_PROCESS_SHARED) != 0 {
            return Err(Error::CondInit(
                "Failed to set shared process attr".to_string(),
            ));
        }

        if libc::pthread_cond_init(cond, cond_attr.as_mut_ptr()) != 0 {
            return Err(Error::CondInit("Failed to init cond".to_string()));
        }
    }

    Ok(())
}

impl<T: Clone> RingBuffer<T> {
    /// Initializes fields and setups locking
    ///
    /// Should only be called once during initial setup of the data structure
    pub fn init(&mut self) -> Result<(), Error> {
        setup_lock(&mut self.lock)?;
        setup_cond(&mut self.has_data)?;
        self.read_pos = 0;
        self.write_pos = 0;
        Ok(())
    }

    /// Puts data into buffer
    ///
    /// - waits for indefinitely for lock
    /// - returns `Error::BufferFull` if there is no space to write
    /// - notifies potential readers via condition of successful write
    fn put(&mut self, data: &T) -> Result<(), Error> {
        unsafe {
            libc::pthread_mutex_lock(&mut self.lock);
        }

        // Check if we can write to buffer
        if (self.write_pos + 1) % BUFFER_SIZE == self.read_pos {
            unsafe {
                libc::pthread_mutex_unlock(&mut self.lock);
            }
            return Err(Error::BufferFull);
        }

        self.buffer[self.write_pos] = data.clone();

        self.write_pos = (self.write_pos + 1) % BUFFER_SIZE;

        unsafe {
            libc::pthread_cond_signal(&mut self.has_data);
            libc::pthread_mutex_unlock(&mut self.lock);
        }

        Ok(())
    }

    /// Gets data from buffer
    ///
    /// - waits for indefinitely for lock
    /// - waits for condition that new data was added if none is there
    /// - notifies potential readers via condition if data is still left to read
    fn get(&mut self) -> Result<T, Error> {
        unsafe {
            libc::pthread_mutex_lock(&mut self.lock);
        }

        // Check if we have something to read otherwise wait
        if self.read_pos == self.write_pos {
            unsafe {
                libc::pthread_cond_wait(&mut self.has_data, &mut self.lock);
            }
        }

        let data = self.buffer[self.read_pos].clone();

        self.read_pos = (self.read_pos + 1) % BUFFER_SIZE;

        // Wake up other threads that still waits for data
        if self.read_pos != self.write_pos {
            unsafe {
                libc::pthread_cond_signal(&mut self.has_data);
            }
        }

        unsafe {
            libc::pthread_mutex_unlock(&mut self.lock);
        }

        Ok(data)
    }
}

#[cfg(test)]

mod tests {
    use super::*;

    const NAME: &str = "testing";

    #[test]
    fn setup() {
        let ipc_server: ShmQueue<u32, u32> =
            ShmQueue::new(NAME, true).expect("Failed to setup Queue");

        let request = Request {
            operation: Operation::Insert,
            key: 1,
            val: 1,
            counter: 0,
        };

        ipc_server
            .request_put(&request)
            .expect("Failed to put things into request buffer");

        ipc_server.stop().expect("unlinking shared memory failed");
    }
}
