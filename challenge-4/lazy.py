



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

    def chunk(self, size):
        """Alias for batch() - groups elements into chunks of specified size"""
        return self.batch(size)

    def page(self, page_number, page_size):
        """Get a specific page of results (1-indexed)"""
        if page_number < 1:
            raise ValueError("Page number must be >= 1")
        offset = (page_number - 1) * page_size
        return self.skip(offset).take(page_size)

    def paginate(self, page_size):
        """Return an iterator of pages, each containing up to page_size elements"""
        page_num = 1
        while True:
            page_data = self.page(page_num, page_size).to_list()
            if not page_data:
                break
            yield page_data
            page_num += 1

    def cache(self, enabled=True):
        c = self._clone()
        c._cache_enabled = enabled
        return c

    # --------- forcing evaluation ----------
    def to_list(self):
        return list(self)

    # --------- reducing operations (force evaluation) ----------
    def reduce(self, fn, initial=None):
        """Apply a function of two arguments cumulatively to items, from left to right"""
        from functools import reduce as builtin_reduce
        items = list(self)
        if initial is not None:
            return builtin_reduce(fn, items, initial)
        else:
            return builtin_reduce(fn, items)

    def sum(self, start=0):
        """Return the sum of all elements"""
        total = start
        for item in self:
            total += item
        return total

    def count(self):
        """Return the count of elements"""
        count = 0
        for _ in self:
            count += 1
        return count

    def min(self, default=None):
        """Return the minimum element"""
        try:
            return min(self)
        except ValueError:
            if default is not None:
                return default
            raise

    def max(self, default=None):
        """Return the maximum element"""
        try:
            return max(self)
        except ValueError:
            if default is not None:
                return default
            raise

    def first(self, default=None):
        """Return the first element, or default if empty"""
        for item in self:
            return item
        return default

    def last(self, default=None):
        """Return the last element, or default if empty"""
        last_item = default
        for item in self:
            last_item = item
        return last_item

    def any(self, pred=None):
        """Return True if any element is truthy (or satisfies predicate)"""
        if pred is None:
            return any(self)
        else:
            return any(pred(x) for x in self)

    def all(self, pred=None):
        """Return True if all elements are truthy (or satisfy predicate)"""
        if pred is None:
            return all(self)
        else:
            return all(pred(x) for x in self)

    def find(self, pred):
        """Return the first element that satisfies the predicate, or None"""
        for item in self:
            if pred(item):
                return item
        return None

    def group_by(self, key_fn):
        """Group elements by the result of key_fn"""
        groups = {}
        for item in self:
            key = key_fn(item)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        return groups

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


