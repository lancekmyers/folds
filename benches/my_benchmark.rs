use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use folds::{
    self,
    common::*,
    fold::{run_fold, run_fold1, run_par_fold, Fold1},
};
use rayon::iter::{IntoParallelIterator, ParallelBridge};

pub fn sum_bench(c: &mut Criterion) {
    // let summer = Sum::SUM;
    let n = 2048;

    let xs: _ = (0..n).collect::<Vec<i32>>().into_iter();
    c.bench_function("fold-sum", |b: _| {
        b.iter(|| run_fold(Sum::SUM, black_box(xs.clone().into_iter())))
    });
    c.bench_function("par-fold-sum", |b: _| {
        b.iter(|| run_par_fold(black_box(xs.clone().par_bridge()), Sum::SUM))
    });
    c.bench_function("vec-sum", move |b| {
        b.iter(|| black_box(xs.clone()).sum::<i32>())
    });
}

pub fn minmax_bench(c: &mut Criterion) {
    // let summer = Sum::SUM;
    let n = 2048;
    let xs: Vec<i32> = (0..n).collect();
    c.bench_function("fold-minmax", |b: _| {
        b.iter(|| run_fold1(Min::MIN.par(Max::MAX), black_box(0..n)))
    });
    c.bench_function("vec-minmax", move |b| {
        b.iter(|| (xs.iter().min(), xs.iter().max()))
    });
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sum");

    for n in [512, 2048, 8192] {
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
