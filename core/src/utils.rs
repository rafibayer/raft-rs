use std::time::Duration;

use rand::Rng;

pub fn rand_duration(range: std::ops::RangeInclusive<Duration>) -> Duration {
    rand::thread_rng().gen_range(range)
}

pub struct RetryOptions {
    pub attempts: usize,
    pub delay: Duration,
}

pub struct RetryError<E> {
    pub inner: Vec<E>
}

pub fn retry<F : Fn() -> Result<T, E>, T, E>(f: F, options: &RetryOptions) -> Result<T, RetryError<E>>  {
    let mut inner = Vec::new();
    for _ in 0..options.attempts {
        match f() {
            Ok(success) => return Ok(success),
            Err(error) => inner.push(error),
        };
    }

    Err(RetryError { inner })
}