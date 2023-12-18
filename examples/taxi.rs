use arrow;
use arrow::csv;
use folds::{
    self,
    fold::{run_fold, run_fold1, run_par_fold, Fold, Fold1, FoldPar},
};
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use std::{
    fs::File,
    io::{BufReader, Error, Seek},
    sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering},
        Arc,
    },
};

fn main() -> () {
    let path = std::env::args().nth(1).unwrap();
    let mut file = File::open(path).unwrap();
    // let mut file = File::open(path).unwrap();
    // Infer the schema with the first 100 records
    let (schema, _) = csv::reader::Format::default()
        .with_header(true)
        .infer_schema(&mut file, Some(100))
        .unwrap();
    file.rewind().unwrap();

    println!("{:?}", schema);

    let batches = csv::reader::ReaderBuilder::new(Arc::new(schema))
        .build(file)
        .unwrap()
        .flatten()
        .collect::<Vec<_>>();

    let avg = folds::common::Sum::SUM
        .par(folds::common::Count::COUNT)
        .post_map(|(tot, cnt)| tot / (cnt as f64));

    println!("Starting iteration");

    batches.par_iter().for_each(|batch| {
        let col = batch.column_by_name("passenger_count").unwrap();
        let foo = col
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        println!("batch avg: {}", run_fold(&avg, foo.into_iter().flatten()))
    });
}
