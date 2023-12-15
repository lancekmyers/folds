use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use folds::{self, common::*, fold::run_fold};

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sum");

    for n in [512, 2048, 8192, 20000] {
        let xs = (0..n).collect::<Vec<i32>>().into_iter();

        group.bench_with_input(BenchmarkId::new("Iterator", n), &xs, |b, xs| {
            b.iter(move || xs.clone().sum::<i32>())
        });

        group.bench_with_input(BenchmarkId::new("Fold", n), &xs.clone(), |b, xs| {
            b.iter(move || run_fold(Sum::SUM, xs.clone()))
        });
    }
    group.finish();
}

// criterion_group!(benches, sum_bench, minmax_bench);
criterion_group!(benches, bench);
criterion_main!(benches);
