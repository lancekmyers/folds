use criterion::{criterion_group, criterion_main, Criterion};
use folds::{
    self,
    common::*,
    fold::{run_fold, run_par_fold},
};
use rayon::iter::ParallelBridge;

pub fn sum_bench(c: &mut Criterion) {
    // let summer = Sum::SUM;
    c.bench_function("fold-sum", |b: _| b.iter(|| run_fold(Sum::SUM, 0..1024)));
    c.bench_function("par-fold-sum", |b: _| {
        b.iter(|| run_par_fold((0..1024).par_bridge(), Sum::SUM))
    });
    c.bench_function("builtin-sum", move |b| {
        b.iter(move || (0..1024).sum::<i32>())
    });
    c.bench_function("vec-sum", move |b| {
        b.iter(move || (0..1024).collect::<Vec<i32>>().iter().sum::<i32>())
    });
}

criterion_group!(benches, sum_bench);
criterion_main!(benches);
