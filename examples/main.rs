use folds::common::*;
use folds::fold::*;

fn main() {
    let xs: Vec<i64> = vec![1, 2, 3, 4, 5];
    let fld = Sum::SUM
        .filter(|x| x % 2 == 0)
        .par(Sum::SUM.group_by(|x| x % 2));

    let fld1 = Min::MIN.par(Max::MAX);

    let (s1, s2) = run_fold(fld, xs.clone().into_iter());

    let (min, max) = run_fold1(fld1, xs.clone().into_iter()).unwrap();

    let (fst, lst) = run_fold1(First::FIRST.par(Last::LAST), xs.into_iter()).unwrap();

    println!("Sum : {}, {:?}", s1, s2);
    println!("Min : {}, Max {}", min, max);

    println!("First : {}, Last {}", fst, lst);

    let avger = Count::COUNT
        .par(Sum::<f64>::SUM)
        .post_map(|(n, sum)| sum / (n as f64));
    let avg = run_fold(avger, (vec![1.0, 2.4, 1.3, 5.1]).into_iter());
    println!("Avg : {avg}")
}
