use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Error},
};

use chrono::{DateTime, Utc};
use rayon::iter::{self, IntoParallelRefIterator};
use rayon::iter::{IntoParallelIterator, ParallelBridge};
use rustc_hash::FxHashMap;
use serde::Deserialize;

use csv;

use itertools::{IntoChunks, Itertools};

use folds::{
    self,
    fold::{run_fold, run_fold1, run_par_fold, Fold, Fold1, FoldPar},
};

#[derive(Deserialize, Clone, Copy)]
struct Trip {
    VendorID: i32,
    // tpep_pickup_datetime: DateTime<Utc>,
    // tpep_dropoff_datetime: DateTime<Utc>,
    #[serde(deserialize_with = "zero_null")]
    passenger_count: f32,
    trip_distance: f32,
    RatecodeID: f32, // should be int
    // store_and_fwd_flag: bool, // ("N"),
    PULocationID: i32,
    DOLocationID: i32,
    payment_type: i32,
    fare_amount: f32,
    extra: f32,
    mta_tax: f32,
    tip_amount: f32,
    tolls_amount: f32,
    improvement_surcharge: f32,
    total_amount: f32,
    congestion_surcharge: f32,
    airport_fee: f32,
}

fn zero_null<'de, D, T: From<u8> + Deserialize<'de>>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Deserialize::deserialize(d).map(|x: Option<_>| x.unwrap_or(T::from(0)))
}

fn main() -> () {
    let path = std::env::args().nth(1).unwrap();
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(reader);
    let records: std::iter::Flatten<csv::DeserializeRecordsIter<'_, BufReader<File>, Trip>> =
        rdr.deserialize().flatten();

    let fld = (folds::common::Sum::<f32>::SUM
        .par(folds::common::Count::COUNT)
        .pre_map(|(_, passenger): (i32, f32)| passenger))
    .group_by(|(vendor, _)| vendor.clone());

    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build_global()
        .unwrap();

    // let grouped_avgs = run_fold(fld, records);

    let grouped_avgs = records
        .map(|trip: Trip| (trip.VendorID, trip.passenger_count))
        .chunks(100000)
        .into_iter()
        .map(|ch| {
            //
            let ch_array: Vec<(i32, f32)> = ch.collect();

            let res: FxHashMap<i32, (f32, u64)> = run_par_fold(ch_array.into_par_iter(), &fld);
            res
        })
        .reduce(|mut x, y| {
            &fld.merge(&mut x, y);
            x
        })
        .unwrap();

    for (vendor, (total, cnt)) in grouped_avgs {
        println!("Average passengers {vendor}: {}", total / (cnt as f32));
    }
}
