use folds::{
    self,
    fold::{Fold, Fold1, FoldPar},
};
use parquet::arrow::async_reader;
use parquet::arrow::ProjectionMask;

use futures::StreamExt;

use rayon::{iter::ParallelIterator, slice::ParallelSlice};

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    let path = args.nth(1).unwrap();
    let file = tokio::fs::File::open(path).await.unwrap();

    let threads: usize = args.next().map(|str| str.parse().unwrap()).unwrap_or(4);
    let batch_size: usize = args.next().map(|str| str.parse().unwrap()).unwrap_or(1024);

    let chunk_size: usize = args.next().map(|str| str.parse().unwrap()).unwrap_or(1024);

    let builder = async_reader::ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .with_batch_size(batch_size);

    let file_metadata = builder.metadata().file_metadata();
    let mask = ProjectionMask::roots(file_metadata.schema_descr(), [3]);

    let stream = builder.with_projection(mask).build().unwrap();

    let fld = folds::stats::CM4::CM4;

    println!("Starting iteration");

    let intermediates: Vec<_> = stream
        .filter_map(|x| async { x.ok() })
        .map(|batch| async move {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            col.values()
                .par_chunks(chunk_size)
                .map(|ch| {
                    let mut acc = fld.empty();
                    fld.step_chunk(ch, &mut acc);
                    acc
                })
                .reduce(
                    || fld.empty(),
                    |mut m1, m2| {
                        fld.merge(&mut m1, m2);
                        m1
                    },
                )
        })
        .buffered(threads)
        .collect()
        .await;

    let ans = fld.output(intermediates.iter().fold(fld.empty(), |mut m1, m2| {
        fld.merge(&mut m1, *m2);
        m1
    }));

    println!("Summary for passenger_count: {:?}", ans);
}
