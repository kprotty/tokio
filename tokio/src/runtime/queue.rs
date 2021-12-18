//! Run-queue structures to support a work-stealing scheduler

use crate::loom::sync::atomic::{self, AtomicPtr, AtomicUsize};
use crate::loom::sync::Arc;
use crate::runtime::stats::WorkerStatsBatcher;
use crate::runtime::task::{self, Inject};

use std::ptr::{null_mut, NonNull};
use std::convert::TryInto;
use std::marker::PhantomData;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// Producer handle. May only be used from a single thread.
pub(super) struct Local<T: 'static> {
    inner: Arc<Inner<T>>,
}

/// Consumer handle. May be used from many threads.
pub(super) struct Steal<T: 'static>(Arc<Inner<T>>);

pub(super) struct Inner<T: 'static> {
    head: AtomicUsize,

    /// Only updated by producer thread but read by many threads.
    tail: AtomicUsize,

    /// Elements
    buffer: Box<[AtomicPtr<task::Header>]>,

    _p: PhantomData<T>,
}

unsafe impl<T> Send for Inner<T> {}
unsafe impl<T> Sync for Inner<T> {}

#[cfg(not(loom))]
const LOCAL_QUEUE_CAPACITY: usize = 256;

// Shrink the size of the local queue when using loom. This shouldn't impact
// logic, but allows loom to test more edge cases in a reasonable a mount of
// time.
#[cfg(loom)]
const LOCAL_QUEUE_CAPACITY: usize = 4;

const MASK: usize = LOCAL_QUEUE_CAPACITY - 1;

/// Create a new local run-queue
pub(super) fn local<T: 'static>() -> (Steal<T>, Local<T>) {
    let buffer = (0..LOCAL_QUEUE_CAPACITY)
        .map(|_| AtomicPtr::new(null_mut()))
        .collect();

    let inner = Arc::new(Inner {
        head: AtomicUsize::new(0),
        tail: AtomicUsize::new(0),
        buffer,
        _p: PhantomData,
    });

    let local = Local {
        inner: inner.clone(),
    };

    let remote = Steal(inner);

    (remote, local)
}

impl<T> Local<T> {
    /// Returns true if the queue has entries that can be stealed.
    pub(super) fn is_stealable(&self) -> bool {
        !self.inner.is_empty()
    }

    /// Returns false if there are any entries in the queue
    ///
    /// Separate to is_stealable so that refactors of is_stealable to "protect"
    /// some tasks from stealing won't affect this
    pub(super) fn has_tasks(&self) -> bool {
        !self.inner.is_empty()
    }

    /// Pushes a task to the back of the local queue, skipping the LIFO slot.
    pub(super) fn push_back(&mut self, task: task::Notified<T>, inject: &Inject<T>) {
        let task = task.into_raw();
        let tail = self.inner.tail.load(Relaxed);
        let mut head = self.inner.head.load(Relaxed);
        
        loop {
            let size = tail.wrapping_sub(head);
            assert!(size <= LOCAL_QUEUE_CAPACITY);

            if size < LOCAL_QUEUE_CAPACITY {
                self.inner.buffer[tail & MASK].store(task.as_ptr(), Relaxed);
                self.inner.tail.store(tail.wrapping_add(1), Release);
                return;
            }

            let migrate = size / 2;
            assert_ne!(migrate, 0);

            if let Err(new_head) = self.inner.head.compare_exchange(
                head,
                head.wrapping_add(migrate),
                Acquire,
                Relaxed,
            ) {
                head = new_head;
                continue;
            }

            let migrated = (0..migrate).map(|i| {
                let slot = &self.inner.buffer[head.wrapping_add(i) & MASK];
                let task = NonNull::new(slot.load(Relaxed)).unwrap();
                unsafe { task::Notified::<T>::from_raw(task) }
            });

            inject.push_batch(migrated);
            return;
        }
    }

    /// Pops a task from the local queue.
    pub(super) fn pop(&mut self) -> Option<task::Notified<T>> {
        let tail = self.inner.tail.load(Relaxed);
        let mut head = self.inner.head.load(Relaxed);

        loop {
            let size = tail.wrapping_sub(head);
            assert!(size <= LOCAL_QUEUE_CAPACITY);

            if size == 0 {
                return None;
            }

            if let Err(new_head) = self.inner.head.compare_exchange(
                head,
                head.wrapping_add(1),
                Acquire,
                Relaxed,
            ) {
                head = new_head;
                continue;
            }

            let slot = &self.inner.buffer[head & MASK];
            let task = NonNull::new(slot.load(Relaxed)).unwrap();
            return Some(unsafe { task::Notified::<T>::from_raw(task) });
        }
    }
}

impl<T> Steal<T> {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Steals half the tasks from self and place them into `dst`.
    pub(super) fn steal_into(
        &self,
        dst: &mut Local<T>,
        stats: &mut WorkerStatsBatcher,
    ) -> Option<task::Notified<T>> {
        let dst_tail = dst.inner.tail.load(Relaxed);
        let dst_head = dst.inner.head.load(Relaxed);

        let dst_size = dst_tail.wrapping_sub(dst_head);
        assert_eq!(dst_size, 0);

        let mut n = loop {
            let head = self.0.head.load(Acquire);
            let tail = self.0.tail.load(Acquire);
            
            if (head == tail) || (head == tail.wrapping_sub(1)) {
                return None;
            }

            let size = tail.wrapping_sub(head);
            let steal = size - (size / 2);
            if steal > LOCAL_QUEUE_CAPACITY / 2 {
                #[allow(deprecated)] atomic::spin_loop_hint();
                continue;
            }

            (0..steal).for_each(|i| {
                let slot = &self.0.buffer[head.wrapping_add(i) & MASK];
                let dst_slot = &dst.inner.buffer[dst_tail.wrapping_add(i) & MASK];
                dst_slot.store(slot.load(Relaxed), Relaxed);
            });

            match self.0.head.compare_exchange(
                head,
                head.wrapping_add(steal),
                AcqRel,
                Relaxed,
            ) {
                Ok(_) => break steal,
                Err(_) => {
                    #[allow(deprecated)] atomic::spin_loop_hint();
                },
            }
        };

        assert_ne!(n, 0);
        stats.incr_steal_count(n.try_into().unwrap());

        n -= 1;
        if n > 0 {
            dst.inner.tail.store(dst_tail.wrapping_add(n), Release);
        }

        let slot = &dst.inner.buffer[dst_tail.wrapping_add(n) & MASK];
        let task = NonNull::new(slot.load(Relaxed)).unwrap();
        Some(unsafe { task::Notified::<T>::from_raw(task) })
    }
}

impl<T> Clone for Steal<T> {
    fn clone(&self) -> Steal<T> {
        Steal(self.0.clone())
    }
}

impl<T> Drop for Local<T> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            assert!(self.pop().is_none(), "queue not empty");
        }
    }
}

impl<T> Inner<T> {
    fn is_empty(&self) -> bool {
        let head = self.head.load(Acquire);
        let tail = self.tail.load(Acquire);

        (head == tail) || (head == tail.wrapping_sub(1))
    }
}

#[test]
fn test_local_queue_capacity() {
    assert!(LOCAL_QUEUE_CAPACITY - 1 <= u8::MAX as usize);
}
