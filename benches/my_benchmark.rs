use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use folds::{
    self,
    common::*,
    fold::{run_fold, run_fold1, Fold1},
};

fn bench_sum(c: &mut Criterion) {
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

fn bench_minmax(c: &mut Criterion) {
    let mut group = c.benchmark_group("MinMax");

    for n in [512, 2048, 8192, 20000] {
        let xs = (0..n).collect::<Vec<i32>>().into_iter();

        group.bench_with_input(BenchmarkId::new("Iterator", n), &xs, |b, xs| {
            b.iter(move || (xs.clone().min(), xs.clone().max()))
        });

        group.bench_with_input(BenchmarkId::new("Fold", n), &xs.clone(), |b, xs| {
            b.iter(move || run_fold1(Min::MIN.par(Max::MAX), xs.clone()))
        });
    }
    group.finish();
}

fn bench_par(c: &mut Criterion) {
    let mut group = c.benchmark_group("Par");

    for n in [512, 2048, 8192, 20000, 40000] {
        let xs = (0..n).collect::<Vec<i32>>().into_iter();

        group.bench_with_input(BenchmarkId::new("Min", n), &xs.clone(), |b, xs| {
            b.iter(move || run_fold1(Min::MIN, xs.clone()))
        });

        group.bench_with_input(BenchmarkId::new("MinMax", n), &xs.clone(), |b, xs| {
            b.iter(move || run_fold1(Min::MIN.par(Max::MAX), xs.clone()))
        });

        group.bench_with_input(BenchmarkId::new("MinMaxSum", n), &xs.clone(), |b, xs| {
            b.iter(move || run_fold1(Min::MIN.par(Max::MAX).par(Sum::SUM), xs.clone()))
        });

        group.bench_with_input(
            BenchmarkId::new("MinMaxSumLast", n),
            &xs.clone(),
            |b, xs| {
                b.iter(move || {
                    run_fold1(
                        Min::MIN.par(Max::MAX).par(Sum::SUM).par(Last::LAST),
                        xs.clone(),
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_sum, bench_minmax, bench_par);
criterion_main!(benches);
