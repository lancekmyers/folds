use arrow::array::Array;
use folds::fold::Fold1 as _;
use folds::{self, fold::run_fold_par_stream, stats::SampleN};
use parquet::arrow::async_reader;
use parquet::arrow::ProjectionMask;

use futures::StreamExt;

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    let path = args.nth(1).unwrap();
    let file = tokio::fs::File::open(path).await.unwrap();

    let threads: usize = args.next().map(|str| str.parse().unwrap()).unwrap_or(4);
    let batch_size: usize = args.next().map(|str| str.parse().unwrap()).unwrap_or(1024);

    let builder = async_reader::ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .with_batch_size(batch_size);

    let file_metadata = builder.metadata().file_metadata().clone();
    let mask = ProjectionMask::roots(file_metadata.schema_descr(), [3]);

    let stream = builder
        .with_projection(mask)
        .build()
        .unwrap()
        .filter_map(|batch| async move {
            let binding = batch.ok()?;
            let prim_arr = binding
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()?;
            Some(Vec::from(&prim_arr.values()[..]))
        });

    let fld = folds::stats::CM4::CM4
        .par(SampleN::<20, f64>::SAMPLE)
        .batched();

    println!("Summary");

    for col in file_metadata.schema_descr().columns() {
        let name = col.path().string();
        let ty = col.physical_type();
        println!("  {:12} {}", ty.to_string(), name)
    }

    println!("passenger_count");
    if let Some(ans) = run_fold_par_stream(&fld, threads, stream).await {
        let ((mu, var, skw, krt), sample) = ans;
        println!(" >>     mean: {:.3}", mu);
        println!(" >>      var: {:.3}", var);
        println!(" >>     skew: {:.3}", skw);
        println!(" >> kurtosis: {:.3}", krt);

        if let Ok(sample) = sample {
            println!(" >> {:?}", sample)
        }
    }
}
