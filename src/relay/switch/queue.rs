use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

pub struct Queue<T> {
    inner: Arc<Mutex<VecDeque<T>>>,
    notify: Arc<Notify>,
}

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            notify: Arc::clone(&self.notify),
        }
    }
}
impl<T> Queue<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::default())),
            notify: Arc::new(Notify::default()),
        }
    }

    pub async fn push(&self, t: T) {
        let mut v = self.inner.lock().await;
        v.push_front(t);
        // Wake all waiters so multiple workers can contend for new jobs
        self.notify.notify_waiters();
    }

    pub async fn pop(&self, count: usize) -> impl IntoIterator<Item = T> {
        loop {
            let mut queue = self.inner.lock().await;

            if !queue.is_empty() {
                let at = queue.len().saturating_sub(count);
                let popped = queue.split_off(at);

                return popped;
            }

            drop(queue);
            self.notify.notified().await;
        }
    }
}
