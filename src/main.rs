use std::marker::PhantomData;

trait Monoid {
    fn id() -> Self;
    fn mul(x: &Self, y: &Self) -> Self;
}

trait Fold {
    type A;
    type B;
    type M;
    fn id(self: &Self) -> Self::M;
    fn inc(self: &Self, x: Self::A, acc: &mut Self::M);
    fn output(self: &Self, acc: Self::M) -> Self::B;
}

impl<A, B, M> dyn Fold<A = A, B = B, M = M> {
    fn map_my_fold<B_>(self: Self, fun: impl Fn(B) -> B_ + 'static) -> Wrapped<A, B_, Self> {
        Wrapped {
            inner: Box::new(self),
            mapped: Box::new(fun),
            comapped: Box::new(|x| x),
        }
    }
}
struct Wrapped<A, B, F: Fold + ?Sized> {
    inner: Box<F>,
    comapped: Box<dyn Fn(A) -> <F as Fold>::A>,
    mapped: Box<dyn Fn(<F as Fold>::B) -> B>,
}

#[derive(Clone, Copy)]
struct SUM<A> {
    not_here: PhantomData<A>,
}

#[derive(Clone, Copy)]
struct PROD<A> {
    not_here: PhantomData<A>,
}

impl<'a, A, B, F: Fold> Fold for Wrapped<A, B, F> {
    type A = A;
    type B = B;
    type M = <F as Fold>::M;

    fn id(self: &Self) -> Self::M {
        self.inner.id()
    }

    fn inc(self: &Self, x: Self::A, acc: &mut Self::M) {
        let x_ = (self.comapped)(x);
        self.inner.inc(x_, acc)
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        let b_ = self.inner.output(acc);
        (self.mapped)(b_)
    }
}

fn add<'a, A, F1, F2, B_, B: std::ops::Add<Output = B_>>(f1: F1, f2: F2) -> Wrapped<A, B_, (F1, F2)>
where
    A: Clone,
    F1: Fold<A = A, B = B>,
    F2: Fold<A = A, B = B>,
{
    Wrapped {
        inner: &(f1, f2),
        comapped: Box::new(|x| x),
        mapped: Box::new(|(x, y)| x + y),
    }
}
fn div<'a, A, F1, F2, B_, B: std::ops::Div<Output = B_>>(f1: F1, f2: F2) -> Wrapped<A, B_, (F1, F2)>
where
    A: Clone,
    F1: Fold<A = A, B = B>,
    F2: Fold<A = A, B = B>,
{
    let f = (f1, f2);
    Wrapped {
        inner: f,
        comapped: Box::new(|x| x),
        mapped: Box::new(|(x, y)| x / y),
    }
}

impl<
        A: Clone,
        B1,
        B2,
        M1,
        M2,
        F1: Fold<A = A, B = B1, M = M1>,
        F2: Fold<A = A, B = B2, M = M2>,
    > Fold for (F1, F2)
{
    type A = A;
    type B = (B1, B2);
    type M = (M1, M2);

    fn id(self: &Self) -> Self::M {
        (self.0.id(), self.1.id())
    }

    fn inc(self: &Self, x: Self::A, (acc1, acc2): &mut Self::M) {
        self.0.inc(x.clone(), acc1);
        self.1.inc(x, acc2);
    }

    fn output(self: &Self, acc: Self::M) -> Self::B {
        let x = self.0.output(acc.0);
        let y = self.1.output(acc.1);
        (x, y)
    }
}

impl<T: From<i32> + std::ops::AddAssign<T>> Fold for SUM<T> {
    type A = T;
    type B = T;
    type M = T;

    fn id(self: &Self) -> T {
        0.into()
    }
    fn inc(self: &Self, x: T, acc: &mut T) {
        *acc += x
    }
    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

impl<T: From<i32> + std::ops::MulAssign<T>> Fold for PROD<T> {
    type A = T;
    type B = T;
    type M = T;

    fn id(self: &Self) -> T {
        1.into()
    }
    fn inc(self: &Self, x: T, acc: &mut T) {
        *acc *= x
    }
    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}
struct Count<T> {
    not_here: PhantomData<T>,
}
impl<T> Fold for Count<T> {
    type A = T;
    type B = i64;
    type M = i64;

    fn id(self: &Self) -> i64 {
        0
    }
    fn inc(self: &Self, _x: T, acc: &mut i64) {
        *acc += 1
    }
    fn output(self: &Self, acc: Self::M) -> Self::B {
        acc
    }
}

fn run_fold<A_, B_, M_, F: Fold<A = A_, B = B_, M = M_>, I: IntoIterator<Item = A_>>(
    folder: &F,
    xs: I,
) -> B_ {
    let mut acc = folder.id();
    xs.into_iter().for_each(|x| folder.inc(x, &mut acc));
    folder.output(acc)
}

fn main() {
    let xs: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let summer = SUM::<f64> {
        not_here: PhantomData,
    };
    let prodder = PROD::<f64> {
        not_here: PhantomData,
    };
    // let counter = (Count::<f64> { not_here : PhantomData }).map_my_fold(|x| x);

    let foo = run_fold(&summer, xs.clone());
    let bar = run_fold(&(summer, prodder), xs.clone());
    let baz = run_fold(&prodder, xs.clone());
    let mean = run_fold(&div(summer, prodder), xs);
    println!("Sum {}", foo);
    println!("Sum {:?}", bar);
    println!("Sum {}", baz);
    println!("mean {mean}");
}
