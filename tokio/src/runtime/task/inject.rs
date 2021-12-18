//! Inject queue used to send wakeups to a work-stealing scheduler

use crate::loom::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use crate::runtime::task;

use std::marker::PhantomData;
use std::ptr::{NonNull, null_mut};
use std::sync::atomic::Ordering::{Acquire, Release, AcqRel, Relaxed};

/// Growable, MPMC queue used to inject new tasks into the scheduler and as an
/// overflow queue when the local, fixed-size, array queue overflows.
pub(crate) struct Inject<T: 'static> {
    /// True if the queue is closed.
    is_closed: AtomicBool,

    is_consuming: AtomicBool,

    pushed: AtomicPtr<task::Header>,

    popped: AtomicPtr<task::Header>,

    /// Number of pending tasks in the queue. This helps prevent unnecessary
    /// locking in the hot path.
    len: AtomicUsize,

    _p: PhantomData<T>,
}

unsafe impl<T> Send for Inject<T> {}
unsafe impl<T> Sync for Inject<T> {}

impl<T: 'static> Inject<T> {
    pub(crate) fn new() -> Inject<T> {
        Inject {
            is_closed: AtomicBool::new(false),
            is_consuming: AtomicBool::new(false),
            pushed: AtomicPtr::new(null_mut()),
            popped: AtomicPtr::new(null_mut()),
            len: AtomicUsize::new(0),
            _p: PhantomData,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Closes the injection queue, returns `true` if the queue is open when the
    /// transition is made.
    pub(crate) fn close(&self) -> bool {
        !self.is_closed.swap(true, AcqRel)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.is_closed.load(Acquire)
    }

    pub(crate) fn len(&self) -> usize {
        self.len.load(Acquire)
    }

    /// Pushes a value into the queue.
    ///
    /// This does nothing if the queue is closed.
    pub(crate) fn push(&self, task: task::Notified<T>) {
        self.push_batch(std::iter::once(task))
    }

    /// Pushes several values into the queue.
    #[inline]
    pub(crate) fn push_batch<I>(&self, mut iter: I)
    where
        I: Iterator<Item = task::Notified<T>>,
    {
        let first = match iter.next() {
            Some(first) => first.into_raw(),
            None => return,
        };

        // Link up all the tasks.
        let mut prev = first;
        let mut counter = 1;

        // We are going to be called with an `std::iter::Chain`, and that
        // iterator overrides `for_each` to something that is easier for the
        // compiler to optimize than a loop.
        iter.for_each(|next| {
            let next = next.into_raw();

            // safety: Holding the Notified for a task guarantees exclusive
            // access to the `queue_next` field.
            set_next(prev, Some(next));
            prev = next;
            counter += 1;
        });

        // Now that the tasks are linked together, insert them into the
        // linked list.
        self.push_batch_inner(first, prev, counter);
    }

    /// Inserts several tasks that have been linked together into the queue.
    ///
    /// The provided head and tail may be be the same task. In this case, a
    /// single task is inserted.
    #[inline]
    fn push_batch_inner(
        &self,
        batch_head: NonNull<task::Header>,
        batch_tail: NonNull<task::Header>,
        num: usize,
    ) {
        debug_assert!(get_next(batch_tail).is_none());

        if self.is_closed() {
            return;
        }

        self.len.fetch_add(num, Acquire);

        let _ = self.pushed.fetch_update(Release, Relaxed, |pushed| {
            set_next(batch_tail, NonNull::new(pushed));
            Some(batch_head.as_ptr())
        });
    }

    pub(crate) fn pop(&self) -> Option<task::Notified<T>> {
        // Fast path, if len == 0, then there are no values
        if self.is_empty() {
            return None;
        }

        if self.is_consuming.swap(true, Acquire) {
            return None;
        }

        let task = NonNull::new(self.popped.load(Relaxed))
            .or_else(|| NonNull::new(self.pushed.swap(null_mut(), Acquire)))
            .map(|task| {
                let next = get_next(task).map(|p| p.as_ptr()).unwrap_or(null_mut());
                self.popped.store(next, Relaxed);
                task
            });

        self.is_consuming.store(false, Release);
        let task = task?;

        self.len.fetch_sub(1, Release);
        Some(unsafe { task::Notified::from_raw(task) })
    }
}

impl<T: 'static> Drop for Inject<T> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            assert!(self.pop().is_none(), "queue not empty");
        }
    }
}

fn get_next(header: NonNull<task::Header>) -> Option<NonNull<task::Header>> {
    unsafe { header.as_ref().queue_next.with(|ptr| *ptr) }
}

fn set_next(header: NonNull<task::Header>, val: Option<NonNull<task::Header>>) {
    unsafe {
        header.as_ref().set_next(val);
    }
}
