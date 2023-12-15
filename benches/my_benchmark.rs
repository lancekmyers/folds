use criterion::{black_box, criterion_group, criterion_main, Criterion};
use folds::{
    self,
    common::*,
    fold::{run_fold, run_fold1, run_par_fold, Fold1},
};
use rayon::iter::IntoParallelIterator;

pub fn sum_bench(c: &mut Criterion) {
    // let summer = Sum::SUM;
    let n = 2048;

    let xs: Vec<i32> = (0..n).collect();
    c.bench_function("fold-sum", |b: _| {
        b.iter(|| run_fold(Sum::SUM, black_box(xs.clone().into_iter())))
    });
    c.bench_function("par-fold-sum", |b: _| {
        b.iter(|| run_par_fold(black_box(xs.clone().into_par_iter()), Sum::SUM))
    });
    c.bench_function("vec-sum", move |b| {
        b.iter(|| black_box(xs.clone().iter()).sum::<i32>())
    });
    c.bench_function("builtin-sum", move |b| {
        b.iter(|| black_box(0..n).sum::<i32>())
    });
}

pub fn minmax_bench(c: &mut Criterion) {
    // let summer = Sum::SUM;
    let n = 2048;
    let xs: Vec<i32> = (0..n).collect();
    c.bench_function("fold-minmax", |b: _| {
        b.iter(|| run_fold1(Min::MIN.par(Max::MAX), black_box(0..n)))
    });
    // c.bench_function("par-fold-minmax", |b: _| {
    //     b.iter(|| run_par_fold(black_box((xs.clone()[..]).par_chunks(2500)), min_max))
    // });
    c.bench_function("vec-minmax", move |b| {
        b.iter(|| (xs.iter().min(), xs.iter().max()))
    });
    c.bench_function("builtin-minmax", move |b| {
        b.iter(|| (black_box(0..n).min(), black_box(0..n).max()))
    });
}

criterion_group!(benches, sum_bench, minmax_bench);

criterion_main!(benches);
