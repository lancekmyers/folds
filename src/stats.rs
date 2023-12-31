use crate::fold::*;
use rand::distributions::Uniform;
use rand::Rng;
use rand::{self, SeedableRng};

/// First 4 central moments
#[derive(Clone, Copy)]
pub struct CM4<A> {
    ghost: std::marker::PhantomData<A>,
}

impl CM4<f64> {
    pub const CM4: Self = CM4 {
        ghost: std::marker::PhantomData,
    };
}

// from https://web.archive.org/web/20140423031833/http://people.xiph.org/~tterribe/notes/homs.html

#[derive(Clone, Copy)]
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

        let delta_n = delta / (*n as f64);

        *m += delta / denom;

        *m4 += delta_n.powi(3) * delta * (denom - 1.0) * (denom.powi(2) - 3.0 * denom + 3.0)
            + 6.0 * (*m2) * delta_n.powi(2)
            - 4.0 * (*m3) * delta_n;
        *m3 += delta_n.powi(2) * delta * (denom - 1.0) * (denom - 2.0) - 3.0 * delta_n * (*m2);

        *m2 += delta_n * delta * (denom - 1.0);
    }

    fn output(&self, acc: Self::M) -> Self::B {
        (
            acc.m,
            acc.m2 / ((acc.n as f64) - 1.0),
            acc.m3 * acc.m2.powf(-1.5) * (acc.n as f64).sqrt(),
            (acc.n as f64) * acc.m4 * acc.m2.powi(-2),
        )
    }
}

impl Fold for CM4<f64> {
    fn empty(&self) -> Self::M {
        MState {
            n: 0,
            m: 0.0,
            m2: 0.0,
            m3: 0.0,
            m4: 0.0,
        }
    }
}

impl FoldPar for CM4<f64> {
    // todo: remove divisions by using delta / nAB
    //   probably will not mattter and might make it harder to read
    fn merge(&self, acc1: &mut Self::M, acc2: Self::M) {
        let n_a = acc1.n as f64;
        let n_b = acc2.n as f64;
        let n_ab = n_a + n_b;
        let delta = acc2.m - acc1.m;
        acc1.n += acc2.n;
        acc1.m += delta * n_b / n_ab;

        let m2_a = acc1.m2;
        let m2_b = acc2.m2;
        acc1.m2 += acc2.m2 + delta * delta * n_a * n_b / n_ab;

        acc1.m3 += acc2.m3
            + delta.powi(3) * n_a * n_b * n_ab.powi(-2) * (n_a - n_b)
            + 3.0 * delta * (n_a * m2_b - n_b * m2_a) / n_ab;

        let m3_a = acc1.m3;
        let m3_b = acc2.m3;
        acc1.m4 += acc2.m4
            + delta.powi(4) * n_a * n_b * (n_a * n_a - n_a * n_b + n_b * n_b) * n_ab.powi(-3)
            + 6.0 * delta * delta * (n_a * n_a * m2_b + n_b * n_b * m2_a) * n_ab.powi(-2)
            + 4.0 * delta * (n_a * m3_b - n_b * m3_a) / n_ab;
    }
}

/// Resevoir sampling using algorithm L
#[derive(Clone, Copy)]
pub struct SampleN<const N: usize, A> {
    ghost: std::marker::PhantomData<A>,
}

impl<const N: usize, A> SampleN<N, A> {
    pub const SAMPLE: Self = SampleN {
        ghost: std::marker::PhantomData,
    };
}

pub enum Resevoir<const N: usize, A> {
    Filling(Vec<A>),
    Resevoir(rand::rngs::SmallRng, f64, usize, [A; N]),
}

impl<const N: usize, A> Resevoir<N, A>
where
    for<'a> [A; N]: TryFrom<&'a mut [A]>,
{
    // TODO: fast sample chunk

    fn new_empty() -> Self {
        Self::Filling(Vec::with_capacity(N))
    }

    fn sample(&mut self, x: A) {
        match self {
            Resevoir::Filling(xs) => {
                xs.push(x);
                if xs.len() == N {
                    let arr: [A; N] = xs.as_mut_slice().try_into().ok().unwrap();
                    let mut rng = rand::rngs::SmallRng::from_entropy();

                    let dist: Uniform<f64> = Uniform::new(0.0, 1.0);

                    let w: f64 = (rng.sample(dist).ln() / (N as f64)).exp();
                    *self = Resevoir::Resevoir(rng, w, 1, arr);
                }
            }

            Resevoir::Resevoir(rng, w, skip, res) => {
                let index_dist: Uniform<usize> = Uniform::new(0, N);

                let dist: Uniform<f64> = Uniform::new(0.0, 1.0);

                let r = rng.sample(dist).ln();
                if r > (-(*w)).ln_1p() {
                    /* Sample */
                } else {
                    /* Skip */
                    *skip += 1;
                }
                if *skip == 0 {
                    let i = rng.sample(index_dist);
                    res[i] = x;
                    *skip += 1;
                }
                *skip -= 1;
                *w *= (rng.sample(dist).ln() / (N as f64)).exp();
            }
        }
    }
}

impl<const N: usize, A> Fold1 for SampleN<N, A>
where
    for<'a> [A; N]: TryFrom<&'a mut [A]>,
{
    type A = A;

    type B = Result<[A; N], Vec<A>>;

    type M = Resevoir<N, A>;

    fn init(&self, x: Self::A) -> Self::M {
        let mut xs = Vec::new();
        Vec::reserve_exact(&mut xs, N);
        xs.push(x);
        Resevoir::Filling(xs)
    }

    fn step(&self, x: Self::A, acc: &mut Self::M) {
        acc.sample(x);
    }

    fn output(&self, acc: Self::M) -> Self::B {
        match acc {
            Resevoir::Filling(xs) => Err(xs),
            Resevoir::Resevoir(_, _, _, res) => Ok(res),
        }
    }
}

impl<const N: usize, A> Fold for SampleN<N, A>
where
    for<'a> [A; N]: TryFrom<&'a mut [A]>,
{
    fn empty(&self) -> Self::M {
        Resevoir::new_empty()
    }
}

impl<const N: usize, A> FoldPar for SampleN<N, A>
where
    for<'a> [A; N]: TryFrom<&'a mut [A]>,
{
    fn merge(&self, m1: &mut Self::M, m2: Self::M) {
        match m2 {
            Resevoir::Filling(xs) => xs.into_iter().for_each(|x| m1.sample(x)),
            Resevoir::Resevoir(_, _, _, res) => res.into_iter().for_each(|x| m1.sample(x)),
        }
    }
}
