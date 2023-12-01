use std::marker::PhantomData;

trait Fold {
    type A;
    type B;
    type M;

    fn new() -> Self;

    fn id(self: &Self) -> Self::M;
    fn inc(self: &Self, x: Self::A, acc: Self::M) -> Self::M;
    fn output(self: &Self, acc: Self::M) -> Self::B;
}

struct Sum<A> {
    ghost: PhantomData<A>,
}

impl<A: std::ops::Add<Output = A> + From<u32>> Fold for Sum<A> {
    type A = A;
    type B = A;
    type M = A;

    fn new() -> Self {
        Sum { ghost: PhantomData }
    }

    fn id(self: &Self) -> Self::M {
        From::from(0)
    }

    fn inc(self: &Self, x: A, acc: A) -> A {
        acc + x
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

fn run_fold<I, O>(fold: impl Fold<A = I, B = O>, xs: impl Iterator<Item = I>) -> O {
    let acc = fold.id();
    let acc_ = xs.fold(acc, |b, i| fold.inc(i, b));
    return fold.output(acc_);
}

fn main() {
    let xs: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let summer = Sum::new();
    let total = run_fold(summer, xs.into_iter());
    println!("Sum : {total}");
}
