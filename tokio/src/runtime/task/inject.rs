//! Inject queue used to send wakeups to a work-stealing scheduler

use crate::loom::sync::atomic::{AtomicUsize, AtomicBool, AtomicPtr};
use crate::runtime::task;

use std::marker::PhantomData;
use std::ptr::{null_mut, NonNull};
use std::sync::atomic::Ordering::{Acquire, Release, AcqRel, Relaxed};

/// Growable, MPMC queue used to inject new tasks into the scheduler and as an
/// overflow queue when the local, fixed-size, array queue overflows.
pub(crate) struct Inject<T: 'static> {
    is_closed: AtomicBool,

    is_consuming: AtomicBool,

    head: AtomicPtr<task::Header>,

    tail: AtomicPtr<task::Header>,

    /// Number of pending tasks in the queue. This helps prevent unnecessary
    /// locking in the hot path.
    len: AtomicUsize,

    _p: PhantomData<T>,
}

impl<T: 'static> Inject<T> {
    pub(crate) fn new() -> Inject<T> {
        Inject {
            is_closed: AtomicBool::new(false),
            is_consuming: AtomicBool::new(false),
            head: AtomicPtr::new(null_mut()),
            tail: AtomicPtr::new(null_mut()),
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
        iter.for_each(|next| unsafe {
            let next = next.into_raw();

            // safety: Holding the Notified for a task guarantees exclusive
            // access to the `queue_next` field.
            prev.as_ref().queue_next.store(next.as_ptr(), Relaxed);
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
        if self.is_closed() {
            return;
        }

        self.len.fetch_add(num, Relaxed);

        unsafe {
            batch_tail.as_ref().queue_next.store(null_mut(), Relaxed);
            let tail = self.tail.swap(batch_tail.as_ptr(), Release);

            let link: *const AtomicPtr<task::Header> = match NonNull::new(tail) {
                Some(tail) => &tail.as_ref().queue_next,
                None => &self.head,
            };

            (&*link).store(batch_head.as_ptr(), Release);
        }
    }

    pub(crate) fn pop(&self) -> Option<task::Notified<T>> {
        // Fast path, if len == 0, then there are no values
        if self.is_empty() {
            return None;
        }

        if self.is_consuming.swap(true, Acquire) {
            return None;
        }

        let task = (|| unsafe {
            let head = NonNull::new(self.head.load(Acquire))?;

            if let Some(next) = NonNull::new(head.as_ref().queue_next.load(Acquire)) {
                self.head.store(next.as_ptr(), Relaxed);
                return Some(head);
            }

            let tail = NonNull::new(self.tail.load(Acquire)).unwrap();
            if head == tail {
                self.head.store(null_mut(), Relaxed);

                match self.tail.compare_exchange(
                    head.as_ptr(),
                    null_mut(),
                    AcqRel,
                    Acquire,
                ) {
                    Ok(_) => return Some(head),
                    Err(_) => self.head.store(head.as_ptr(), Relaxed),
                }
            }

            let next = NonNull::new(head.as_ref().queue_next.load(Acquire))?;
            self.head.store(next.as_ptr(), Relaxed);
            Some(head)
        })();

        if task.is_some() {
            self.len.fetch_sub(1, Relaxed);
        }

        self.is_consuming.store(false, Release);

        // safety: a `Notified` is pushed into the queue and now it is popped!
        task.map(|task| unsafe { task::Notified::from_raw(task) })
    }
}

impl<T: 'static> Drop for Inject<T> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            assert!(self.pop().is_none(), "queue not empty");
        }
    }
}

