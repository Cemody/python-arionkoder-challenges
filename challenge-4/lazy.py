



class LazyCollection:
    """
    A chainable, lazy collection. Transformations are stored and applied
    only when you iterate. Optionally supports caching of realized results.
    """
    def __init__(self, source, ops=None, cache_enabled=False):
        self._source = source
        self._ops = ops or []          # sequence of ("op_name", callable/arg)
        self._cache_enabled = cache_enabled
        self._cache = []               # realized items (post-ops)
        self._exhausted = False        # whether we've fully iterated (when caching)

    # --------- chainable operators (lazy) ----------
    def map(self, fn):
        return self._with_op(("map", fn))

    def filter(self, pred):
        return self._with_op(("filter", pred))

    def skip(self, n):
        return self._with_op(("skip", int(n)))

    def take(self, n):
        return self._with_op(("take", int(n)))

    def batch(self, size):
        return self._with_op(("batch", int(size)))

    def cache(self, enabled=True):
        c = self._clone()
        c._cache_enabled = enabled
        return c

    # --------- forcing evaluation ----------
    def to_list(self):
        return list(self)

    # --------- iterator protocol ----------
    def __iter__(self):
        # If caching is on, yield any cached values first
        if self._cache_enabled:
            for item in self._cache:
                yield item
            if self._exhausted:
                return  # we're done; no more source to compute

        # Build the pipeline starting from the (possibly once-iterable) source
        it = iter(self._source)
        for op, arg in self._ops:
            if op == "map":
                fn = arg
                it = (fn(x) for x in it)
            elif op == "filter":
                pred = arg
                it = (x for x in it if pred(x))
            elif op == "skip":
                k = arg
                def _skip(gen, k=k):
                    skipped = 0
                    for x in gen:
                        if skipped < k:
                            skipped += 1
                            continue
                        yield x
                it = _skip(it)
            elif op == "take":
                n = arg
                def _take(gen, n=n):
                    taken = 0
                    for x in gen:
                        if taken >= n:
                            return
                        yield x
                        taken += 1
                it = _take(it)
            elif op == "batch":
                size = arg
                def _batch(gen, size=size):
                    bucket = []
                    for x in gen:
                        bucket.append(x)
                        if len(bucket) == size:
                            yield tuple(bucket)
                            bucket = []
                    if bucket:
                        yield tuple(bucket)
                it = _batch(it)
            else:
                raise ValueError(f"Unknown op: {op}")

        # If caching: as we produce new items, append to cache
        if self._cache_enabled:
            for item in it:
                self._cache.append(item)
                yield item
            self._exhausted = True
        else:
            yield from it

    # --------- helpers ----------
    def _with_op(self, op_tuple):
        return LazyCollection(self._source, self._ops + [op_tuple], self._cache_enabled)

    def _clone(self):
        c = LazyCollection(self._source, list(self._ops), self._cache_enabled)
        # Note: cache is not shared when cloning via .cache(); we want independent caches per pipeline
        return c


