use std::marker::PhantomData;

trait Fold {
    type A;
    type B;
    type M;

    fn new() -> Self;

    fn empty(self: &Self) -> Self::M;
    fn step(self: &Self, x: Self::A, acc: &mut Self::M);
    fn output(self: &Self, acc: Self::M) -> Self::B;
}

fn run_fold<I, O>(fold: impl Fold<A = I, B = O>, xs: impl Iterator<Item = I>) -> O {
    let mut acc = fold.empty();
    xs.for_each(|i| fold.step(i, &mut acc));
    return fold.output(acc);
}

trait Fold1 {
    type A;
    type B;
    type M;

    fn new() -> Self;
    fn init(self: &Self, x: Self::A) -> Self::M;
    fn step(self: &Self, x: Self::A, acc: &mut Self::M);
    fn output(self: &Self, acc: Self::M) -> Self::B;
}

fn run_fold1<I, O>(fold: impl Fold1<A = I, B = O>, mut xs: impl Iterator<Item = I>) -> Option<O> {
    if let Some(first) = xs.next() {
        let mut acc = fold.init(first);
        xs.for_each(|i| fold.step(i, &mut acc));
        return Some(fold.output(acc));
    } else {
        return None;
    }
}

struct Sum<A> {
    ghost: PhantomData<A>,
}

impl<A: std::ops::AddAssign + From<u32>> Fold for Sum<A> {
    type A = A;
    type B = A;
    type M = A;

    fn new() -> Self {
        Sum { ghost: PhantomData }
    }

    fn empty(self: &Self) -> Self::M {
        From::from(0)
    }

    fn step(self: &Self, x: A, acc: &mut A) {
        *acc += x
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

struct Max<A> {
    ghost: PhantomData<A>,
}

impl<A: std::cmp::Ord> Fold1 for Max<A> {
    type A = A;

    type B = A;

    type M = A;

    fn new() -> Self {
        Max { ghost: PhantomData }
    }

    fn init(self: &Self, x: A) -> Self::M {
        x
    }

    fn step(self: &Self, x: A, acc: &mut A) -> () {
        if x < *acc {
        } else {
            *acc = x;
        }
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

struct Min<A> {
    ghost: PhantomData<A>,
}

impl<A: std::cmp::Ord> Fold1 for Min<A> {
    type A = A;

    type B = A;

    type M = A;

    fn new() -> Self {
        Min { ghost: PhantomData }
    }

    fn init(self: &Self, x: A) -> Self::M {
        x
    }

    fn step(self: &Self, x: A, acc: &mut A) {
        if x > *acc {
        } else {
            *acc = x;
        }
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

struct First<A> {
    ghost: PhantomData<A>,
}

impl<A> Fold1 for First<A> {
    type A = A;
    type B = A;
    type M = A;

    fn new() -> Self {
        First { ghost: PhantomData }
    }

    fn init(self: &Self, x: A) -> Self::M {
        x
    }

    fn step(self: &Self, _x: A, _acc: &mut A) {}

    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

struct Last<A> {
    ghost: PhantomData<A>,
}

impl<A> Fold1 for Last<A> {
    type A = A;
    type B = A;
    type M = A;

    fn new() -> Self {
        Last { ghost: PhantomData }
    }

    fn init(self: &Self, x: A) -> Self::M {
        x
    }

    fn step(self: &Self, x: A, acc: &mut A) {
        *acc = x;
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

struct Par2<F1, F2> {
    f1: F1,
    f2: F2,
}

impl<I: Copy, F1: Fold<A = I>, F2: Fold<A = I>> Fold for Par2<F1, F2> {
    type A = I;

    type B = (F1::B, F2::B);

    type M = (F1::M, F2::M);

    fn new() -> Self {
        Par2 {
            f1: F1::new(),
            f2: F2::new(),
        }
    }

    fn empty(self: &Self) -> Self::M {
        (self.f1.empty(), self.f2.empty())
    }

    fn step(self: &Self, x: Self::A, (acc1, acc2): &mut (<F1 as Fold>::M, <F2 as Fold>::M)) {
        self.f1.step(x, acc1);
        self.f2.step(x, acc2);
    }

    fn output(self: &Self, (acc1, acc2): Self::M) -> Self::B {
        (self.f1.output(acc1), self.f2.output(acc2))
    }
}

impl<I: Copy, F1: Fold1<A = I>, F2: Fold1<A = I>> Fold1 for Par2<F1, F2> {
    type A = I;

    type B = (F1::B, F2::B);

    type M = (F1::M, F2::M);

    fn new() -> Self {
        Par2 {
            f1: F1::new(),
            f2: F2::new(),
        }
    }

    fn init(self: &Self, x: Self::A) -> Self::M {
        let i1 = self.f1.init(x);
        let i2 = self.f2.init(x);

        (i1, i2)
    }

    fn step(self: &Self, x: Self::A, (acc1, acc2): &mut Self::M) {
        self.f1.step(x, acc1);
        self.f2.step(x, acc2);
    }

    fn output(self: &Self, (a1, a2): Self::M) -> Self::B {
        (self.f1.output(a1), self.f2.output(a2))
    }
}

fn main() {
    let xs: Vec<i64> = vec![1, 2, 3, 4, 5];
    let fld = Par2::<Sum<_>, Sum<_>>::new();

    let (s1, s2) = run_fold(fld, xs.into_iter());

    println!("Sum : {}, {}", s1, s2);
}
