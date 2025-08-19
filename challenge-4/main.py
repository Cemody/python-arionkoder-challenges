from time import sleep, perf_counter
from lazy import LazyCollection

def expensive_transform(x):
    # Simulate a costly step so laziness is visible
    print(f"  computing f({x}) ...")
    sleep(0.2)  # pretend this is expensive
    return x * x

print("\n--- Demo: laziness (no work until iterated) ---")
data = range(1, 10_000)  # big-ish source
pipeline = (
    LazyCollection(data)
    .map(expensive_transform)   # expensive; watch when it runs
    .filter(lambda v: v % 2 == 0)
    .skip(3)
    .take(5)
)

print("Constructed pipeline. No output yet (nothing computed).")
print("\nIterating (should compute only what's needed for 5 items):")
t0 = perf_counter()
out = list(pipeline)  # forces just enough work to get 5 items
t1 = perf_counter()
print(f"Result: {out}")
print(f"Time: {t1 - t0:.2f}s\n")

print("--- Demo: partial consumption stays lazy ---")
pipeline2 = (
    LazyCollection(range(1, 30))
    .map(expensive_transform)
    .filter(lambda v: v % 3 == 0)
)
print("Taking only the first 3:")
t0 = perf_counter()
from itertools import islice
first_three = list(islice(pipeline2, 3))
t1 = perf_counter()
print(f"First three: {first_three} (computed only what was needed). Time: {t1 - t0:.2f}s\n")

print("--- Demo: batching ---")
batched = (
    LazyCollection(range(1, 12))
    .map(expensive_transform)
    .batch(4)
    .take(2)  # only first two batches -> only first 8 items computed
)
print("Two batches of 4 (should compute exactly 8 items):")
for b in batched:
    print("  batch:", b)
print()

print("--- Demo: caching (first pass computes; second pass reuses) ---")
cached_pipeline = (
    LazyCollection(range(1, 12))
    .map(expensive_transform)
    .filter(lambda v: v % 5 != 0)
    .cache(True)  # turn on memoization of realized results
)

print("First pass (computes):")
t0 = perf_counter()
_ = list(cached_pipeline)  # realize all
t1 = perf_counter()
print(f"First pass time: {t1 - t0:.2f}s\n")

print("Second pass (should be instant; no recomputation):")
t0 = perf_counter()
_ = list(cached_pipeline)  # reuse cache
t1 = perf_counter()
print(f"Second pass time: {t1 - t0:.4f}s")

