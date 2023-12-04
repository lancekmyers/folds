use std::hash::Hash;
use std::marker::PhantomData;

use std::collections::HashMap;

pub trait Fold1 {
    type A;
    type B;
    type M;

    fn init(&self, x: Self::A) -> Self::M;
    fn step(&self, x: Self::A, acc: &mut Self::M);
    fn output(&self, acc: Self::M) -> Self::B;

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

    fn filter<Pred>(self, pred: Pred) -> FilteredFold<Self, Pred>
    where
        Self: Sized,
        Pred: Fn(&Self::A) -> bool,
    {
        FilteredFold { inner: self, pred }
    }

    fn par<F2>(self, f2: F2) -> Par2<Self, F2>
    where
        F2: Fold1<A = Self::A> + Sized,
        Self: Sized,
    {
        Par2 { f1: self, f2 }
    }

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
}

pub trait Fold: Fold1 {
    fn empty(&self) -> Self::M;
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
