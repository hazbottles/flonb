import pytest

import flonb
from flonb.task import Cache


def test_set_cache_dir():
    Cache._reset()

    @flonb.task_func(cache_disk=True)
    def test_func():
        pass

    with pytest.raises(ValueError) as excinfo:
        test_func.compute()
    assert "Set cache dir with `flonb.set_cache_dir`." in str(excinfo)


def test_cache_disk(tmpdir):
    flonb.set_cache_dir(tmpdir.strpath)

    _counts = [0]

    @flonb.task_func(cache_disk=True)
    def dummy_func():
        _counts[0] += 1  # side effect - if cached, won't be executed

    dummy_func.compute()
    dummy_func.compute()
    assert _counts[0] == 1

    dummy_func()  # call directly
    assert _counts[0] == 2


def test_cache_disk_with_options_and_deps(tmpdir):
    flonb.set_cache_dir(tmpdir.strpath)

    _add_one_xs = []
    _multiply_ys = []

    @flonb.task_func()
    def add_one(x):
        _add_one_xs.append(x)
        return x + 1

    @flonb.task_func(cache_disk=True)
    def multiply(y, base=flonb.Dep(add_one)):
        _multiply_ys.append(y)
        return base * y

    @flonb.task_func(cache_disk=True)
    def add_z(z, base=flonb.Dep(multiply)):
        return base + z

    assert multiply.compute(x=3, y=2) == 8
    assert multiply.compute(x=3, y=2) == 8  # cached result
    assert multiply.compute(x=2, y=3) == 9
    # checks that upstream parameter 'x' is included in cache identifiers
    assert multiply.compute(x=5, y=3) == 18
    # x=2 and y=2 have been computed separately
    assert multiply.compute(x=2, y=2) == 6

    assert add_z.compute(x=5, y=3, z=3) == 21  # cached result (x=5, y=3)
    assert add_z.compute(x=5, y=3, z=7) == 25  # cached result (x=5, y=3)
    assert add_z.compute(x=1, y=4, z=3) == 11  # new

    assert _add_one_xs == [3, 2, 5, 2, 1]
    assert _multiply_ys == [2, 3, 3, 2, 4]
