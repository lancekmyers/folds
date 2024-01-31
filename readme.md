# Folds 

This is meant to be a first class rust folds library like the classic 
Haskell library
[foldl](https://hackage.haskell.org/package/foldl).

This allows the user to build up a fold from smaller pieces. 
For example, you could combine the `sum` fold and the `count` fold
to compute the average.

```{rust}
// sum : Fold<A = f64, B = f64>
// cnt : Fold<A = f64, B = usize>
let avg = sum.par(cnt).post_map(|(tot, n)| tot / (n as f64))
```

I am not an experienced rust programmer, so this may be a horribly unidiomatic design. 
The api is roughly modeled after the iterators api. 

I would *strongly advise against* using this for anything important.

## Anatomy of a Fold 

A simplified version of the `Fold` trait looks like 

```{rust}
trait Fold {
    // Input type, ie the type of the elements to be folded over
    type A; 
    // Output type, ie the type of the final result
    type B; 
    // Intermediate state
    type M; 
    
    // Initialize the state
    fn init(&self) -> Self::M;
    // Update the internal state
    fn step(&self, &mut m : Self::M, x : Self::A);
    // Finalize, i.e. extract and clean up the useful bits from the state 
    fn output(&self, m : Self::M) -> Self::B;
}
```

A simple example might be a fold for summing integers. 

```{rust}
struct SumI32 {}
impl Fold for SumI32 {
    type A = i32;
    type B = i32;
    type M = i32;
    
    fn init(&self) -> i32 {
        0
    }
    fn step(&self, acc : &mut i32, x : i32) {
        *acc += x
    }
    fn output(&self, acc : i32) -> i32 {
        acc
    }
}
```

From this we can build a new fold that sums only the even elements. 

```{rust}
let sum = SumI32 {};
let sum_even = sum.filter(|x| x % 2 == 0);
```

We can also run two folds side by side in one pass.

```{rust}
let sum_odd = sum.filter(|x) x % 2 != 0);
let sum_even_odd = sum_even.par(sum_odd);

let (sum_of_even_elts, sum_of_odd_elts) = run_fold(sum_even_odd, (0..25));
```

When the fold is run, everything is computed in one pass, making it 
suitable for streaming applications.

## Roadmap

The resevoir sampler is a little finicky and I think there may be a bug.
I would like to add more testing in general, but especially for the sampler. 

In the near future I would like to implement
- HyperLogLog 
- CountMin Sketch 
- t-digest 
