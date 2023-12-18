use std::{
    fs::File,
    io::{BufReader, Error},
};

use chrono::{DateTime, Utc};
use rayon::iter::ParallelBridge;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Value};

use folds::{
    self,
    fold::{run_fold1, run_par_fold, Fold1},
};

#[derive(Deserialize)]
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

    let stream = Deserializer::from_reader(reader).into_iter::<Trip>();

    let fld = folds::common::Sum::SUM
        .par(folds::common::Count::COUNT)
        .pre_map(|x: Trip| x.passenger_count);

    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build_global()
        .unwrap();

    let (total, cnt) = run_par_fold(stream.flatten().par_bridge(), fld);
    println!("Average passengers: {}", total / (cnt as f32));
}
