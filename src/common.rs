use crate::fold::*;

use std::marker::PhantomData;

pub struct Sum<A> {
    ghost: PhantomData<A>,
}

impl<A: std::ops::AddAssign<A> + From<u32>> Sum<A> {
    pub const SUM: Self = Sum { ghost: PhantomData };
}

impl<A: std::ops::AddAssign> Fold1 for Sum<A> {
    type A = A;
    type B = A;
    type M = A;

    fn init(&self, x: Self::A) -> Self::M {
        x
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        *acc += x
    }

    fn output(&self, acc: Self::M) -> Self::B {
        acc
    }
}

impl<A: std::ops::AddAssign + From<u32>> Fold for Sum<A> {
    fn empty(&self) -> Self::M {
        From::from(0)
    }
}

pub struct Max<A> {
    ghost: PhantomData<A>,
}

impl<A: std::cmp::Ord> Max<A> {
    pub const MAX: Self = Max { ghost: PhantomData };
}

impl<A: std::cmp::Ord> Fold1 for Max<A> {
    type A = A;

    type B = A;

    type M = A;

    fn init(&self, x: A) -> Self::M {
        x
    }

    fn step(&self, x: A, acc: &mut A) {
        if x < *acc {
        } else {
            *acc = x;
        }
    }

    fn output(&self, acc: Self::M) -> Self::B {
        acc
    }
}

pub struct Min<A> {
    ghost: PhantomData<A>,
}

impl<A: std::cmp::Ord> Min<A> {
    pub const MIN: Self = Min { ghost: PhantomData };
}

impl<A: std::cmp::Ord> Fold1 for Min<A> {
    type A = A;

    type B = A;

    type M = A;

    fn init(&self, x: A) -> Self::M {
        x
    }

    fn step(&self, x: A, acc: &mut A) {
        if x > *acc {
        } else {
            *acc = x;
        }
    }

    fn output(&self, acc: Self::M) -> Self::B {
        acc
    }
}

pub struct First<A> {
    ghost: PhantomData<A>,
}

impl<A> First<A> {
    pub const FIRST: Self = First { ghost: PhantomData };
}

impl<A> Fold1 for First<A> {
    type A = A;
    type B = A;
    type M = A;

    fn init(&self, x: A) -> Self::M {
        x
    }

    fn step(&self, _x: A, _acc: &mut A) {}

    fn output(&self, acc: Self::M) -> Self::B {
        acc
    }
}

pub struct Last<A> {
    ghost: PhantomData<A>,
}

impl<A> Last<A> {
    pub const LAST: Self = Last { ghost: PhantomData };
}

impl<A> Fold1 for Last<A> {
    type A = A;
    type B = A;
    type M = A;

    fn init(&self, x: A) -> Self::M {
        x
    }

    fn step(&self, x: A, acc: &mut A) {
        *acc = x;
    }

    fn output(&self, acc: Self::M) -> Self::B {
        acc
    }
}

pub struct Count<A> {
    ghost: PhantomData<A>,
}

impl<A> Count<A> {
    pub const COUNT: Self = Count { ghost: PhantomData };
}

impl<A> Fold1 for Count<A> {
    type A = A;
    type B = u64;
    type M = u64;

    fn init(&self, _x: Self::A) -> Self::M {
        1
    }
    fn step(&self, _x: Self::A, acc: &mut Self::M) {
        *acc += 1;
    }
    fn output(&self, acc: Self::M) -> Self::B {
        acc
    }
}

impl<A> Fold for Count<A> {
    fn empty(&self) -> Self::M {
        0
    }
}
