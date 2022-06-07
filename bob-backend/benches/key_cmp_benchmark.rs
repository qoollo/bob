use bob_backend::pearl::le_cmp_keys;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("cmp size 8", |b| {
        b.iter(|| {
            le_cmp_keys_opt(
                black_box(&[3, 1, 1, 1, 1, 1, 1, 1]),
                black_box(&[2, 1, 1, 1, 1, 1, 1, 1]),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
