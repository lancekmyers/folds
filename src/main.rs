use std::hash::Hash;
use std::marker::PhantomData;

use std::collections::HashMap;

trait Fold1 {
    type A;
    type B;
    type M;

    fn init(self: &Self, x: Self::A) -> Self::M;
    fn step(self: &Self, x: Self::A, acc: &mut Self::M);
    fn output(self: &Self, acc: Self::M) -> Self::B;

    fn group_by<GetKey, Key>(self: Self, get_key: GetKey) -> GroupedFold<Self, GetKey>
    where
        Self: Sized,
        Key: Hash + Eq,
        GetKey: Fn(&Self::A) -> Key,
    {
        GroupedFold {
            inner: self,
            get_key,
        }
    }

    fn filter<Pred>(self: Self, pred: Pred) -> FilteredFold<Self, Pred>
    where
        Self: Sized,
        Pred: Fn(&Self::A) -> bool,
    {
        FilteredFold { inner: self, pred }
    }

    fn par<F2>(self: Self, f2: F2) -> Par2<Self, F2>
    where
        F2: Fold1<A = Self::A> + Sized,
        Self: Sized,
    {
        Par2 { f1: self, f2: f2 }
    }

    fn pre_map<A2, PreFunc>(self: Self, pre_func: PreFunc) -> PreMap<Self, A2, PreFunc>
    where
        Self: Sized,
        PreFunc: Fn(A2) -> Self::A,
    {
        PreMap {
            inner: self,
            pre_func: pre_func,
            ghost: PhantomData::<A2>,
        }
    }

    fn post_map<B2, PostFunc>(self: Self, post_func: PostFunc) -> PostMap<Self, B2, PostFunc>
    where
        Self: Sized,
        PostFunc: Fn(Self::B) -> B2,
    {
        PostMap {
            inner: self,
            post_func: post_func,
        }
    }
}

trait Fold: Fold1 {
    fn empty(self: &Self) -> Self::M;
}

fn run_fold<I, O>(fold: impl Fold<A = I, B = O>, xs: impl Iterator<Item = I>) -> O {
    let mut acc = fold.empty();
    xs.for_each(|i| fold.step(i, &mut acc));
    return fold.output(acc);
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

impl<A: std::ops::AddAssign<A> + From<u32>> Sum<A> {
    const SUM: Self = Sum { ghost: PhantomData };
}

impl<A: std::ops::AddAssign> Fold1 for Sum<A> {
    type A = A;
    type B = A;
    type M = A;

    fn init(self: &Self, x: Self::A) -> Self::M {
        x
    }

    fn step(self: &Self, x: Self::A, acc: &mut Self::M) {
        *acc += x
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

impl<A: std::ops::AddAssign + From<u32>> Fold for Sum<A> {
    fn empty(self: &Self) -> Self::M {
        From::from(0)
    }
}

struct Max<A> {
    ghost: PhantomData<A>,
}

impl<A: std::cmp::Ord> Max<A> {
    const MAX: Self = Max { ghost: PhantomData };
}

impl<A: std::cmp::Ord> Fold1 for Max<A> {
    type A = A;

    type B = A;

    type M = A;

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

impl<A: std::cmp::Ord> Min<A> {
    const MIN: Self = Min { ghost: PhantomData };
}

impl<A: std::cmp::Ord> Fold1 for Min<A> {
    type A = A;

    type B = A;

    type M = A;

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

impl<A> First<A> {
    const FIRST: Self = First { ghost: PhantomData };
}

impl<A> Fold1 for First<A> {
    type A = A;
    type B = A;
    type M = A;

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

impl<A> Last<A> {
    const LAST: Self = Last { ghost: PhantomData };
}

impl<A> Fold1 for Last<A> {
    type A = A;
    type B = A;
    type M = A;

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

impl<I: Copy, F1: Fold1<A = I>, F2: Fold1<A = I>> Fold1 for Par2<F1, F2> {
    type A = I;
    type B = (F1::B, F2::B);
    type M = (F1::M, F2::M);

    fn init(self: &Self, x: Self::A) -> Self::M {
        (self.f1.init(x), self.f2.init(x))
    }

    fn step(self: &Self, x: Self::A, (acc1, acc2): &mut (<F1 as Fold1>::M, <F2 as Fold1>::M)) {
        self.f1.step(x, acc1);
        self.f2.step(x, acc2);
    }

    fn output(self: &Self, (acc1, acc2): Self::M) -> Self::B {
        (self.f1.output(acc1), self.f2.output(acc2))
    }
}

impl<I: Copy, F1: Fold<A = I>, F2: Fold<A = I>> Fold for Par2<F1, F2> {
    fn empty(self: &Self) -> Self::M {
        (self.f1.empty(), self.f2.empty())
    }
}

struct FilteredFold<F, P> {
    inner: F,
    pred: P,
}

impl<F: Fold1, P: Fn(&F::A) -> bool> Fold1 for FilteredFold<F, P> {
    type A = F::A;
    type B = F::B;
    type M = F::M;

    fn step(self: &Self, x: Self::A, acc: &mut Self::M) {
        if (self.pred)(&x) {
            self.inner.step(x, acc)
        }
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        self.inner.output(acc)
    }

    fn init(self: &Self, x: Self::A) -> Self::M {
        self.inner.init(x)
    }
}

impl<F: Fold, P: Fn(&F::A) -> bool> Fold for FilteredFold<F, P> {
    fn empty(self: &Self) -> Self::M {
        self.inner.empty()
    }
}

struct GroupedFold<F, GetKey> {
    inner: F,
    get_key: GetKey,
}

impl<F: Fold1, Key: Hash + Eq, GetKey: Fn(&F::A) -> Key> Fold1 for GroupedFold<F, GetKey>
where
    F::A: Copy, // This should not be necessary
{
    type A = F::A;
    type B = HashMap<Key, F::B>;
    type M = HashMap<Key, F::M>;

    fn init(self: &Self, x: Self::A) -> Self::M {
        HashMap::new()
    }

    fn step(self: &Self, x: Self::A, acc: &mut Self::M) {
        let key = (self.get_key)(&x);

        acc.entry(key)
            .and_modify(|v| self.inner.step(x, v))
            .or_insert(self.inner.init(x));
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc.into_iter()
            .map(|(k, m)| (k, self.inner.output(m)))
            .collect()
    }
}

impl<F: Fold, Key: Hash + Eq, GetKey: Fn(&F::A) -> Key> Fold for GroupedFold<F, GetKey>
where
    F::A: Copy,
{
    fn empty(self: &Self) -> Self::M {
        HashMap::new()
    }
}

struct PreMap<F: Fold1, A2, PreFunc: Fn(A2) -> F::A> {
    inner: F,
    pre_func: PreFunc,
    ghost: PhantomData<A2>,
}

impl<F: Fold1, A2, PreFunc: Fn(A2) -> F::A> Fold1 for PreMap<F, A2, PreFunc> {
    type A = A2;
    type B = F::B;
    type M = F::M;

    fn init(self: &Self, x: Self::A) -> Self::M {
        self.inner.init((self.pre_func)(x))
    }

    fn step(self: &Self, x: Self::A, acc: &mut Self::M) {
        self.inner.step((self.pre_func)(x), acc)
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        self.inner.output(acc)
    }
}

impl<F: Fold, A2, PreFunc: Fn(A2) -> F::A> Fold for PreMap<F, A2, PreFunc> {
    fn empty(self: &Self) -> Self::M {
        self.inner.empty()
    }
}

struct PostMap<F: Fold1, B2, PostFunc: Fn(F::B) -> B2> {
    inner: F,
    post_func: PostFunc,
}

impl<F: Fold1, B2, PostFunc: Fn(F::B) -> B2> Fold1 for PostMap<F, B2, PostFunc> {
    type A = F::A;
    type B = B2;
    type M = F::M;

    fn init(self: &Self, x: Self::A) -> Self::M {
        self.inner.init(x)
    }

    fn step(self: &Self, x: Self::A, acc: &mut Self::M) {
        self.inner.step(x, acc)
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        (self.post_func)(self.inner.output(acc))
    }
}

impl<F: Fold, B2, PostFunc: Fn(F::B) -> B2> Fold for PostMap<F, B2, PostFunc> {
    fn empty(self: &Self) -> Self::M {
        self.inner.empty()
    }
}

fn mk_summer<A: std::ops::AddAssign + From<u32>>() -> Sum<A> {
    Sum { ghost: PhantomData }
}

fn mk_minner<A: std::cmp::Ord>() -> Min<A> {
    Min { ghost: PhantomData }
}

fn mk_maxer<A: std::cmp::Ord>() -> Max<A> {
    Max { ghost: PhantomData }
}

// This is a simple version of a scan that doesn't really work
// because filtered folds will break.
// Consider scan(filtered(summer, is_odd), xs)
// this will return an iterator the same length as the input
// it wont really allow for filtering
fn scan<F: Fold>(fld: F, iter: impl Iterator<Item = F::A>) -> impl Iterator<Item = F::B>
where
    F::M: Copy,
{
    let mut acc = fld.empty();
    iter.map(move |x| {
        fld.step(x, &mut acc);
        return fld.output(acc);
    })
}

fn main() {
    let xs: Vec<i64> = vec![1, 2, 3, 4, 5];
    let fld = Sum::SUM
        .filter(|x| x % 2 == 0)
        .par(Sum::SUM.group_by(|x| x % 2));

    let fld1 = Min::MIN.par(Max::MAX);

    let (s1, s2) = run_fold(fld, xs.clone().into_iter());

    let (min, max) = run_fold1(fld1, xs.clone().into_iter()).unwrap();

    println!("Sum : {}, {:?}", s1, s2);
    println!("Min : {}, Max {}", min, max);
}
