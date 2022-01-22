use std::time::Duration;

use rand::Rng;

pub fn rand_duration(range: std::ops::RangeInclusive<Duration>) -> Duration {
    rand::thread_rng().gen_range(range)
}
