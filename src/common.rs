use crate::fold::*;

use std::marker::PhantomData;

#[derive(Copy, Clone)]
pub struct Sum<A> {
    ghost: PhantomData<A>,
}

impl<A: std::ops::AddAssign<A> + From<u8>> Sum<A> {
    pub const SUM: Self = Sum { ghost: PhantomData };
}

impl<A: std::ops::AddAssign> Fold1 for Sum<A>
where
    A: for<'a> std::iter::Sum<&'a A>,
{
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

    fn step_chunk(&self, xs: &[Self::A], acc: &mut Self::M)
    where
        Self::A: Copy,
    {
        *acc += xs.iter().sum();
    }
}

impl<A: std::ops::AddAssign + From<u8>> Fold for Sum<A>
where
    A: for<'a> std::iter::Sum<&'a A>,
{
    fn empty(&self) -> Self::M {
        From::from(0)
    }
}

impl<A: std::ops::AddAssign + From<u8>> FoldPar for Sum<A>
where
    A: for<'a> std::iter::Sum<&'a A>,
{
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        *m1 += m2
    }
}

#[derive(Copy, Clone)]
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

impl<A: std::cmp::Ord> FoldPar for Max<A> {
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        if *m1 > m2 {
        } else {
            *m1 = m2
        }
    }
}

#[derive(Copy, Clone)]
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

impl<A: std::cmp::Ord> FoldPar for Min<A> {
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        if *m1 < m2 {
        } else {
            *m1 = m2
        }
    }
}

#[derive(Copy, Clone)]
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

#[derive(Copy, Clone)]
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

#[derive(Copy, Clone)]
pub struct Count<A> {
    ghost: PhantomData<A>,
}

impl<A> Count<A> {
    pub const COUNT: Self = Count { ghost: PhantomData };
}

impl<A> Fold1 for Count<A> {
    type A = A;
    type B = usize;
    type M = usize;

    fn init(&self, _x: Self::A) -> Self::M {
        1
    }
    fn step(&self, _x: Self::A, acc: &mut Self::M) {
        *acc += 1;
    }
    fn output(&self, acc: Self::M) -> Self::B {
        acc
    }

    fn step_chunk(&self, xs: &[Self::A], acc: &mut Self::M)
    where
        Self::A: Copy,
    {
        *acc += xs.len();
    }
}

impl<A> Fold for Count<A> {
    fn empty(&self) -> Self::M {
        0
    }
}

impl<A> FoldPar for Count<A> {
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        *m1 += m2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn iota(n: usize) -> Vec<usize> {
        (0..n).collect()
    }

    #[test]
    fn sum_min_max() {
        fn go(n: usize) {
            let expected = (n * (n - 1) / 2, (0usize, n - 1));
            let fld = Sum::SUM.par(Min::MIN.par(Max::MAX));
            let ans = run_fold1_iter(&fld, iota(n).into_iter());
            assert_eq!(ans.unwrap(), expected)
        }

        for n in [2, 50, 500] {
            go(n)
        }
    }

    #[test]
    fn fst_lst_cnt() {
        fn go(n: usize) {
            let expected = (0usize, (n - 1, n));
            let fld = First::FIRST.par(Last::LAST.par(Count::COUNT));
            let ans = run_fold1_iter(&fld, iota(n).into_iter());
            assert_eq!(ans.unwrap(), expected)
        }

        for n in [2, 50, 500] {
            go(n)
        }
    }
}
