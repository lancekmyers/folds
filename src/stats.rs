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

impl FoldPar for CM4<f64> {
    // todo: remove divisions by using delta / nAB
    //   probably will not mattter and might make it harder to read
    fn merge(&self, acc1: &mut Self::M, acc2: Self::M) {
        let nA = acc1.n as f64;
        let nB = acc2.n as f64;
        let nAB = nA + nB;
        let delta = acc1.m - acc2.m;
        acc1.n += acc2.n;
        acc1.m += delta * nB / nAB;

        let m2A = acc1.m2;
        let m2B = acc2.m2;
        acc1.m2 += acc2.m2 + delta * delta * nA * nB / nAB;

        acc1.m3 += acc2.m3
            + delta.powi(3) * nA * nB * nAB.powi(-2) * (nA - nB)
            + 3.0 * delta * (nA * m2B - nB * m2A) / nAB;

        let m3A = acc1.m3;
        let m3B = acc2.m3;
        acc1.m4 += acc2.m4
            + delta.powi(4) * nA * nB * (nA * nA - nA * nB + nB * nB) * nAB.powi(-3)
            + 6.0 * delta * delta * (nA * nA * m2B + nB * nB * m2A) * nAB.powi(-2)
            + 4.0 * delta * (nA * m3B - nB * m3A) / nAB;
    }
}
