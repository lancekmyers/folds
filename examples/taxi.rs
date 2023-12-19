use arrow;
use folds::{
    self,
    fold::{run_fold, run_fold1, run_par_fold, Fold, Fold1, FoldPar},
};
use parquet::arrow::async_reader;
use parquet::arrow::{arrow_reader, ProjectionMask};

use futures::{stream, StreamExt, TryStreamExt};

use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};

#[tokio::main]
async fn main() -> () {
    let path = std::env::args().nth(1).unwrap();
    let file = tokio::fs::File::open(path).await.unwrap();

    let builder = async_reader::ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .with_batch_size(3);

    let file_metadata = builder.metadata().file_metadata();
    let mask = ProjectionMask::roots(file_metadata.schema_descr(), [3]);

    let stream = builder.with_projection(mask).build().unwrap();

    let avg = folds::common::Sum::SUM
        .par(folds::common::Count::COUNT)
        .post_map(|(tot, cnt)| tot / (cnt as f64));

    println!("Starting iteration");

    let foo: Vec<(f64, u64)> = stream
        .filter_map(|x| async { x.ok() })
        .map(|batch| {
            tokio::task::spawn(async move {
                let col = batch.column(0);
                let foo = col
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                let mut acc = (&avg).empty();
                (&avg).step_chunk(foo.values(), &mut acc);
                acc
            })
        })
        .buffered(4)
        .try_collect()
        .await
        .unwrap();

    let bar = foo.iter().fold((&avg).empty(), |mut m1, m2| {
        (&avg).merge(&mut m1, *m2);
        m1
    });
    let ans = avg.output(bar);
    // let ans: f64 = avg.output(
    //     foo.into_iter()
    //         .reduce(|mut m1, m2| {
    //             avg.merge(&mut m1, m2);
    //             m1
    //         })
    //         .unwrap(),
    // );
    println!("Average passenger_count: {ans}");
}
