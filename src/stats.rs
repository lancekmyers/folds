use crate::fold::*;

// First 4 central moments
pub struct CM4<A> {
    ghost: std::marker::PhantomData<A>,
}

// from https://web.archive.org/web/20140423031833/http://people.xiph.org/~tterribe/notes/homs.html

pub struct MState<A> {
    n: usize,
    m: A,
    m2: A,
    m3: A,
    m4: A,
}

impl Fold1 for CM4<f64> {
    type A = f64;

    type B = (f64, f64, f64, f64);

    type M = MState<f64>;

    fn init(&self, x: Self::A) -> Self::M {
        MState {
            n: 1,
            m: x,
            m2: x,
            m3: x,
            m4: x,
        }
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        let MState { n, m, m2, m3, m4 } = acc;

        let delta = x - *m;
        *n += 1;
        let denom: f64 = *n as f64;
        *m += delta / denom;
        *m2 += delta / denom * delta * (denom - 1.0);
        *m3 += delta * delta * delta * ((denom - 1.0) * (denom - 2.0) / (denom * denom))
            - 3.0 * delta * (*m2) / denom;
        *m4 += delta.powi(4) * denom.powi(-3) * (denom - 1.0) * (denom.powi(2) - 3.0 * denom + 3.0)
            + 6.0 * (*m2) * (delta / denom).powi(2)
            - 4.0 * (*m3) * delta / denom;
    }

    fn output(&self, acc: Self::M) -> Self::B {
        (
            acc.m,
            acc.m2 / ((acc.n as f64) - 1.0),
            acc.m3 * acc.m2.powf(1.5) * (acc.n as f64).sqrt(),
            (acc.n as f64) * acc.m4 * acc.m2.powi(-2),
        )
    }
}
