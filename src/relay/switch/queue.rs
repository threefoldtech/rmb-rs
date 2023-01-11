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
        self.notify.notify_one();
    }

    pub async fn pop(&self, count: usize) -> impl IntoIterator<Item = T> {
        loop {
            let mut queue = self.inner.lock().await;

            if queue.len() > 0 {
                let at = queue.len().saturating_sub(count);
                let popped = queue.split_off(at);

                return popped;
            }

            drop(queue);
            self.notify.notified().await;
        }
    }

    pub async fn pop_no_wait(&self, count: usize) -> impl IntoIterator<Item = T> {
        let mut queue = self.inner.lock().await;

        let at = queue.len().saturating_sub(count);
        queue.split_off(at)
    }
}

#[cfg(test)]
mod test {
    use super::Queue;

    #[tokio::test]
    async fn test_queue() {
        let queue: Queue<u32> = Queue::new();
        queue.push(1).await;
        queue.push(2).await;
        queue.push(3).await;
        let mut v = queue.pop_no_wait(0).await.into_iter();
        assert_eq!(v.next(), None);

        let mut v = queue.pop_no_wait(1).await.into_iter();
        assert_eq!(v.next(), Some(1));
        assert_eq!(v.next(), None);

        let mut v = queue.pop_no_wait(4).await.into_iter();
        assert_eq!(v.next(), Some(3));
        assert_eq!(v.next(), Some(2));
        assert_eq!(v.next(), None);

        let mut v = queue.pop_no_wait(4).await.into_iter();
        assert_eq!(v.next(), None);
    }

    #[tokio::test]
    async fn test_queue_order() {
        let queue: Queue<u32> = Queue::new();
        queue.push(1).await;
        queue.push(2).await;
        queue.push(3).await;

        let mut v = queue.pop_no_wait(4).await.into_iter();
        assert_eq!(v.next(), Some(3));
        assert_eq!(v.next(), Some(2));
        assert_eq!(v.next(), Some(1));
        assert_eq!(v.next(), None);
    }
}
