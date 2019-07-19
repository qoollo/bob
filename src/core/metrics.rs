extern crate dipstick;
use dipstick::*;
use std::time::Duration;

// metrics! {
//     pub SOME_COUNTER: Counter = "some_counter";
// }

metrics! {
    GRINDER: Proxy = "grinder" => {
        pub GRINDER_PUT_COUNTER: Counter = "put_count";
        pub GRINDER_PUT_TIMER: Timer = "put_timer";

        pub GRINDER_GET_COUNTER: Counter = "get_count";
        pub GRINDER_GET_TIMER: Timer = "get_timer";
    }
}

metrics! {
    CLIENT: Proxy = "client" => {
        pub CLIENT_PUT_COUNTER: Counter = "put_count";
        pub CLIENT_PUT_TIMER: Timer = "put_timer";

        pub CLIENT_GET_COUNTER: Counter = "get_count";
        pub CLIENT_GET_TIMER: Timer = "get_timer";
    }
}

pub fn init_counters(prefix: &str, output: impl Output + Send + Sync + Clone + 'static, duration: Duration) {
    init_grinder(&(prefix.to_owned() + "cluster"), output.clone(), duration);
    init_bob_client(&(prefix.to_owned() + "backend"), output.clone(), duration);
}

pub fn init_grinder(prefix: &str, output: impl Output + Send + Sync + Clone + 'static, duration: Duration) {
    let bucket = AtomicBucket::new().named( prefix.to_string());
    
    bucket.stats(stats_all_bob);
    bucket.drain(output.clone());

    bucket.flush_every(duration);

    GRINDER.target(bucket.clone());
}

pub fn init_bob_client(prefix: &str, output: impl Output + Send + Sync + Clone + 'static, duration: Duration) {
    let bucket = AtomicBucket::new().named(prefix.to_string());
    
    bucket.stats(stats_all_bob);
    bucket.drain(output.clone());

    bucket.flush_every(duration);

    CLIENT.target(bucket.clone());
}

fn stats_all_bob(
    kind: InputKind,
    name: MetricName,
    score: ScoreType,
) -> Option<(InputKind, MetricName, MetricValue)> {
    match score {
        ScoreType::Count(hit) => Some((InputKind::Counter, name.make_name("count"), hit)),
        ScoreType::Sum(sum) => Some((kind, name.make_name("sum"), sum)),
        ScoreType::Mean(mean) => Some((kind, name.make_name("mean"), mean as MetricValue)),
        ScoreType::Max(max) => Some((InputKind::Gauge, name.make_name("max"), max)),
        ScoreType::Min(min) => Some((InputKind::Gauge, name.make_name("min"), min)),
        ScoreType::Rate(rate) => Some((
            InputKind::Gauge,
            name.make_name("rate"),
            rate as MetricValue,
        )),
    }
}