use std::hash::Hash;
use std::marker::PhantomData;

use rustc_hash::FxHashMap;

use rayon;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};

use futures::{self, Stream, StreamExt};

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

    /// Update rule for state given new chunk of data
    /// Allows for better performance via simd + better cach behaviour
    fn step_chunk(&self, xs: Vec<Self::A>, acc: &mut Self::M) {
        for x in xs {
            self.step(x, acc)
        }
    }

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

    /// Batched version of a fold, ie the input type is now
    /// chunks of data. This can be useful for vectorization
    fn batched(self) -> Batched<Self>
    where
        Self: Sized,
        Self::A: Clone,
    {
        Batched { inner: self }
    }

    /// Paralellizes a fold with itself over a wide stream
    fn many(self, n: usize) -> Many<Self>
    where
        Self: Sized,
    {
        Many { inner: self, n: n }
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

pub fn run_fold_iter<I, O>(fold: &impl Fold<A = I, B = O>, xs: impl Iterator<Item = I>) -> O {
    let mut acc = fold.empty();
    xs.for_each(|i| fold.step(i, &mut acc));
    fold.output(acc)
}

pub fn run_fold1_iter<I, O>(
    fold: &impl Fold1<A = I, B = O>,
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

/// Run a fold over a stream of values
pub async fn run_fold_stream<O, I>(fold: &impl Fold<A = I, B = O>, xs: impl Stream<Item = I>) -> O {
    fold.output(
        xs.fold(fold.empty(), |mut acc, x| async move {
            fold.step(x, &mut acc);
            acc
        })
        .await,
    )
}

/// Run a fold over a stream of values in parallel
pub async fn run_fold_par_stream<O, I, F>(
    fold: &F,
    j: usize,
    xs: impl StreamExt<Item = I>,
) -> Option<O>
where
    F: Fold<A = I, B = O> + FoldPar + Send + Sync + Clone + 'static,
    F::M: Send + Sync,
    I: Send + 'static,
{
    Some(
        fold.output(
            xs.map(move |x| {
                let f = fold.clone();
                tokio::task::spawn_blocking(move || f.init(x))
            })
            .buffered(j)
            .fold(fold.empty(), |mut m1, m2| async move {
                if let Ok(m2) = m2 {
                    fold.merge(&mut m1, m2);
                }
                m1
            })
            .await,
        ),
    )
}

/// Run a fold over a parallel iterator of values
pub fn run_fold_par_iter<I, O, F>(iter: impl IndexedParallelIterator<Item = I>, fold: &F) -> O
where
    F: FoldPar + Fold<A = I, B = O> + Sync,
    F::M: Send,
{
    fold.output(
        iter.chunks(1024)
            .map(|ch| {
                let mut acc = fold.empty();
                ch.into_iter().for_each(|i| fold.step(i, &mut acc));
                acc
            })
            .reduce(
                || fold.empty(),
                |mut m1, m2| {
                    fold.merge(&mut m1, m2);
                    m1
                },
            ),
    )
}

pub fn run_fold1_par_iter<I, O, F>(
    iter: impl IndexedParallelIterator<Item = I>,
    fold: &F,
) -> Option<O>
where
    F: FoldPar + Fold<A = I, B = O> + Sync,
    F::M: Send + Copy,
    I: Copy,
{
    let mut accs: Vec<_> = iter
        .chunks(1024)
        .map(|mut ch| {
            let rest = ch.drain(1..).collect();
            let x0 = ch.get(0)?;
            let mut acc = fold.init(*x0);
            fold.step_chunk(rest, &mut acc);
            Some(acc)
        })
        .filter_map(|x| x)
        .collect();

    // let mut a0 = accs.get(0)?;
    // let mut a0 = accs[0];
    let (a0, rest) = accs.split_first_mut()?;
    for a in rest {
        fold.merge(a0, *a);
    }
    Some(fold.output(*a0))
}

#[derive(Copy, Clone)]
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

    fn step_chunk(&self, xs: Vec<Self::A>, (acc1, acc2): &mut Self::M)
    where
        Self::A: Copy,
    {
        self.f1.step_chunk(xs.clone(), acc1);
        self.f2.step_chunk(xs, acc2);
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

#[derive(Copy, Clone)]
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

    fn step_chunk(&self, xs: Vec<Self::A>, acc: &mut Self::M) {
        // This cannot be close to optimal
        // I should not pay this allocation each time
        // Consider passing mask to step_chunk
        let mut xs_in = Vec::with_capacity(xs.len() / 2);

        for x in xs {
            if (self.pred)(&x) {
                xs_in.push(x)
            }
        }
        self.inner.step_chunk(xs_in, acc);
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

#[derive(Copy, Clone)]
pub struct GroupedFold<F, GetKey> {
    inner: F,
    get_key: GetKey,
}

impl<F: Fold1, Key: Hash + Eq, GetKey: Fn(&F::A) -> Key> Fold1 for GroupedFold<F, GetKey> {
    type A = F::A;
    type B = FxHashMap<Key, F::B>;
    type M = FxHashMap<Key, F::M>;

    fn init(&self, _x: Self::A) -> Self::M {
        FxHashMap::default()
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        let key = (self.get_key)(&x);

        if let Some(m) = acc.get_mut(&key) {
            self.inner.step(x, m);
        } else {
            acc.insert(key, self.inner.init(x));
        }
    }

    // fn step_chunk()
    // It should be possible to do something clever here,
    // but if you have moderately high cardinality within
    // each chunk, then I think it might just be overhead

    fn output(&self, acc: Self::M) -> Self::B {
        acc.into_iter()
            .map(|(k, m)| (k, self.inner.output(m)))
            .collect()
    }
}

impl<F: Fold, Key: Hash + Eq, GetKey: Fn(&F::A) -> Key> Fold for GroupedFold<F, GetKey> {
    fn empty(&self) -> Self::M {
        FxHashMap::default()
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

#[derive(Copy, Clone)]
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

#[derive(Copy, Clone)]
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

#[derive(Copy, Clone)]
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

#[derive(Clone, Copy)]
pub struct Batched<F: Fold1> {
    inner: F,
}
impl<A: Clone, F: Fold<A = A>> Fold1 for Batched<F> {
    type A = Vec<F::A>;

    type B = F::B;

    type M = F::M;

    // this will panic on empty chunk
    fn init(&self, x: Self::A) -> Self::M {
        let mut acc = self.inner.empty();
        self.inner.step_chunk(x, &mut acc);
        acc
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        self.inner.step_chunk(x, acc)
    }

    fn output(&self, acc: Self::M) -> Self::B {
        self.inner.output(acc)
    }
}

impl<A: Clone, F: Fold<A = A>> Fold for Batched<F> {
    fn empty(&self) -> Self::M {
        self.inner.empty()
    }
}

impl<A: Clone, F: FoldPar<A = A> + Fold> FoldPar for Batched<F> {
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        self.inner.merge(m1, m2)
    }
}

/// Perform a fold in parallel with itself over a wide stream
pub struct Many<F: Fold1> {
    inner: F,
    n: usize,
}
impl<F: Fold1> Fold1 for Many<F> {
    type A = Vec<F::A>;

    type B = Vec<F::B>;

    type M = Vec<F::M>;

    fn init(&self, x: Self::A) -> Self::M {
        x.into_iter().map(|x| self.inner.init(x)).collect()
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        for (mut a, x) in acc.into_iter().zip(x.into_iter()) {
            self.inner.step(x, &mut a)
        }
    }

    fn output(&self, acc: Self::M) -> Self::B {
        acc.into_iter().map(|a| self.inner.output(a)).collect()
    }
}

impl<F: Fold> Fold for Many<F> {
    fn empty(&self) -> Self::M {
        let mut accs = Vec::with_capacity(self.n);
        for _ in 0..self.n {
            accs.push(self.inner.empty());
        }
        accs
    }
}
