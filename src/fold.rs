use std::hash::Hash;
use std::marker::PhantomData;

use std::collections::HashMap;

use rayon;
use rayon::iter::ParallelIterator;

/// Trait representing that something can be seen as a "fold1", i.e.
/// a fold that will always be given at least one input.
pub trait Fold1 {
    /// Input type
    type A;
    // Output type
    type B;
    /// Intermediate internal state
    type M;

    /// Initialize state given first element
    fn init(&self, x: Self::A) -> Self::M;
    /// Update rule for state given new piece of data
    fn step(&self, x: Self::A, acc: &mut Self::M);
    /// Final step to clean up internal state and present it to the
    /// outside world.
    /// Often this is simply the identity function if no transformation
    /// needs to happen.
    fn output(&self, acc: Self::M) -> Self::B;

    /// Perform fold grouped by a key.
    /// Resulting output type is a HashMap
    fn group_by<GetKey, Key>(self, get_key: GetKey) -> GroupedFold<Self, GetKey>
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

    /// Only fold over input values satiisfying the given predicate.
    fn filter<Pred>(self, pred: Pred) -> FilteredFold<Self, Pred>
    where
        Self: Sized,
        Pred: Fn(&Self::A) -> bool,
    {
        FilteredFold { inner: self, pred }
    }

    /// Perform this fold in parallel with another.
    /// The second fold must have the same (copyable) input type.
    /// The resulting output type will be a pair.
    fn par<F2>(self, f2: F2) -> Par2<Self, F2>
    where
        F2: Fold1<A = Self::A> + Sized,
        Self::A: Copy,
        Self: Sized,
    {
        Par2 { f1: self, f2 }
    }

    /// Apply a function to all inputs.
    /// Note that this changes the input type of the fold.
    /// This is a contravariant functor fmap
    fn pre_map<A2, PreFunc>(self, pre_func: PreFunc) -> PreMap<Self, A2, PreFunc>
    where
        Self: Sized,
        PreFunc: Fn(A2) -> Self::A,
    {
        PreMap {
            inner: self,
            pre_func,
            ghost: PhantomData::<A2>,
        }
    }

    /// Apply a function to the output.
    /// Note that this changes the output type of the fold.
    /// This is a covariant functor fmap
    fn post_map<B2, PostFunc>(self, post_func: PostFunc) -> PostMap<Self, B2, PostFunc>
    where
        Self: Sized,
        PostFunc: Fn(Self::B) -> B2,
    {
        PostMap {
            inner: self,
            post_func,
        }
    }

    /// Compose two folds
    /// This folds the second over the scan of the first
    fn then<F2: Fold<A = Self::B>>(self, next: F2) -> ComposedFold<Self, F2>
    where
        Self: Sized,
        Self::M: Copy,
    {
        ComposedFold {
            first: self,
            second: next,
        }
    }
}

pub trait Fold: Fold1 {
    fn empty(&self) -> Self::M;
}

/// Folds whose intermediate state can be merged,
/// allowing for parallel folds
pub trait FoldPar: Fold1 {
    fn merge(&self, m1: &mut Self::M, m2: Self::M);
}

pub fn run_fold<I, O>(fold: impl Fold<A = I, B = O>, xs: impl Iterator<Item = I>) -> O {
    let mut acc = fold.empty();
    xs.for_each(|i| fold.step(i, &mut acc));
    fold.output(acc)
}

pub fn run_fold1<I, O>(
    fold: impl Fold1<A = I, B = O>,
    mut xs: impl Iterator<Item = I>,
) -> Option<O> {
    if let Some(first) = xs.next() {
        let mut acc = fold.init(first);
        xs.for_each(|i| fold.step(i, &mut acc));
        Some(fold.output(acc))
    } else {
        None
    }
}

pub fn run_par_fold<I, O, F>(iter: impl rayon::iter::ParallelIterator<Item = I>, fold: F) -> O
where
    F: FoldPar + Fold<A = I, B = O> + Sync,
    F::M: Send,
{
    fold.output(
        iter.fold(
            || fold.empty(),
            |mut acc, x| {
                fold.step(x, &mut acc);
                acc
            },
        )
        .reduce(
            || fold.empty(),
            |mut m1, m2| {
                fold.merge(&mut m1, m2);
                m1
            },
        ),
    )
}

pub struct Par2<F1, F2> {
    f1: F1,
    f2: F2,
}

impl<I: Copy, F1: Fold1<A = I>, F2: Fold1<A = I>> Fold1 for Par2<F1, F2> {
    type A = I;
    type B = (F1::B, F2::B);
    type M = (F1::M, F2::M);

    fn init(&self, x: Self::A) -> Self::M {
        (self.f1.init(x), self.f2.init(x))
    }

    fn step(&self, x: Self::A, (acc1, acc2): &mut (<F1 as Fold1>::M, <F2 as Fold1>::M)) {
        self.f1.step(x, acc1);
        self.f2.step(x, acc2);
    }

    fn output(&self, (acc1, acc2): Self::M) -> Self::B {
        (self.f1.output(acc1), self.f2.output(acc2))
    }
}

impl<I: Copy, F1: Fold<A = I>, F2: Fold<A = I>> Fold for Par2<F1, F2> {
    fn empty(&self) -> Self::M {
        (self.f1.empty(), self.f2.empty())
    }
}

impl<F1: FoldPar, F2: FoldPar> FoldPar for Par2<F1, F2>
where
    Par2<F1, F2>: Fold1<M = (F1::M, F2::M)>,
{
    fn merge(&self, (m11, m12): &mut Self::M, (m21, m22): Self::M) {
        self.f1.merge(m11, m21);
        self.f2.merge(m12, m22);
    }
}

pub struct FilteredFold<F, P> {
    inner: F,
    pred: P,
}

impl<F: Fold1, P: Fn(&F::A) -> bool> Fold1 for FilteredFold<F, P> {
    type A = F::A;
    type B = F::B;
    type M = F::M;

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        if (self.pred)(&x) {
            self.inner.step(x, acc)
        }
    }

    fn output(&self, acc: Self::M) -> Self::B {
        self.inner.output(acc)
    }

    fn init(&self, x: Self::A) -> Self::M {
        self.inner.init(x)
    }
}

impl<F: Fold, P: Fn(&F::A) -> bool> Fold for FilteredFold<F, P> {
    fn empty(&self) -> Self::M {
        self.inner.empty()
    }
}

impl<F: FoldPar, P: Fn(&F::A) -> bool> FoldPar for FilteredFold<F, P> {
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        self.inner.merge(m1, m2)
    }
}

pub struct GroupedFold<F, GetKey> {
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

    fn init(&self, _x: Self::A) -> Self::M {
        HashMap::new()
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        let key = (self.get_key)(&x);

        acc.entry(key)
            .and_modify(|v| self.inner.step(x, v))
            .or_insert(self.inner.init(x));
    }

    fn output(&self, acc: Self::M) -> Self::B {
        acc.into_iter()
            .map(|(k, m)| (k, self.inner.output(m)))
            .collect()
    }
}

impl<F: Fold, Key: Hash + Eq, GetKey: Fn(&F::A) -> Key> Fold for GroupedFold<F, GetKey>
where
    F::A: Copy,
{
    fn empty(&self) -> Self::M {
        HashMap::new()
    }
}

impl<F: FoldPar, Key: Hash + Eq, GetKey: Fn(&F::A) -> Key> FoldPar for GroupedFold<F, GetKey>
where
    F::A: Copy,
{
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        for (k, v) in m2.into_iter() {
            if let Some(v1) = m1.get_mut(&k) {
                self.inner.merge(v1, v);
            } else {
                m1.insert(k, v);
            }
        }
    }
}

pub struct PreMap<F: Fold1, A2, PreFunc: Fn(A2) -> F::A> {
    inner: F,
    pre_func: PreFunc,
    ghost: PhantomData<A2>,
}

impl<F: Fold1, A2, PreFunc: Fn(A2) -> F::A> Fold1 for PreMap<F, A2, PreFunc> {
    type A = A2;
    type B = F::B;
    type M = F::M;

    fn init(&self, x: Self::A) -> Self::M {
        self.inner.init((self.pre_func)(x))
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        self.inner.step((self.pre_func)(x), acc)
    }

    fn output(&self, acc: Self::M) -> Self::B {
        self.inner.output(acc)
    }
}

impl<F: Fold, A2, PreFunc: Fn(A2) -> F::A> Fold for PreMap<F, A2, PreFunc> {
    fn empty(&self) -> Self::M {
        self.inner.empty()
    }
}

impl<F: FoldPar, A2, PreFunc: Fn(A2) -> F::A> FoldPar for PreMap<F, A2, PreFunc> {
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        self.inner.merge(m1, m2)
    }
}

pub struct PostMap<F: Fold1, B2, PostFunc: Fn(F::B) -> B2> {
    inner: F,
    post_func: PostFunc,
}

impl<F: Fold1, B2, PostFunc: Fn(F::B) -> B2> Fold1 for PostMap<F, B2, PostFunc> {
    type A = F::A;
    type B = B2;
    type M = F::M;

    fn init(&self, x: Self::A) -> Self::M {
        self.inner.init(x)
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        self.inner.step(x, acc)
    }

    fn output(&self, acc: Self::M) -> Self::B {
        (self.post_func)(self.inner.output(acc))
    }
}

impl<F: Fold, B2, PostFunc: Fn(F::B) -> B2> Fold for PostMap<F, B2, PostFunc> {
    fn empty(&self) -> Self::M {
        self.inner.empty()
    }
}

impl<F: FoldPar, B2, PostFunc: Fn(F::B) -> B2> FoldPar for PostMap<F, B2, PostFunc> {
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        self.inner.merge(m1, m2)
    }
}

pub struct ComposedFold<F1: Fold1, F2: Fold1> {
    first: F1,
    second: F2,
}

impl<F1: Fold1, F2: Fold1<A = F1::B>> Fold1 for ComposedFold<F1, F2>
where
    F1::M: Copy,
{
    type A = F1::A;

    type B = F2::B;

    type M = (F1::M, F2::M);

    fn init(&self, x: Self::A) -> Self::M {
        let m1 = self.first.init(x);
        let m2 = self.second.init(self.first.output(m1));
        (m1, m2)
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        let (m1, m2) = acc;
        self.first.step(x, m1);
        let y = self.first.output(*m1);
        self.second.step(y, m2);
    }

    fn output(&self, acc: Self::M) -> Self::B {
        let (_m1, m2) = acc;
        self.second.output(m2)
    }
}

impl<F1: Fold, F2: Fold1<A = F1::B>> Fold for ComposedFold<F1, F2>
where
    F1::M: Copy,
{
    fn empty(&self) -> Self::M {
        let m1 = self.first.empty();
        let m2 = self.second.init(self.first.output(m1));
        (m1, m2)
    }
}

// This is a simple version of a scan that doesn't really work
// because filtered folds will break.
// Consider scan(filtered(summer, is_odd), xs)
// this will return an iterator the same length as the input
// it wont really allow for filtering
#[allow(dead_code)]
pub fn scan<F: Fold>(fld: F, iter: impl Iterator<Item = F::A>) -> impl Iterator<Item = F::B>
where
    F::M: Copy,
{
    let mut acc = fld.empty();
    iter.map(move |x| {
        fld.step(x, &mut acc);
        fld.output(acc)
    })
}
