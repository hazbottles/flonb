import inspect

import flonb


def test___call__():
    @flonb.task_func()
    def add(x, y):
        return x + y

    assert add(1, 4) == 5


def test___call___with_dependency():
    @flonb.task_func()
    def str_to_int(x_str: str):
        return int(x_str)

    @flonb.task_func()
    def add(y, x=flonb.Dep(str_to_int)):
        return x + y

    assert add(2, 4) == 6


def test_signature():
    def test_func(a, b, c=3):
        """This is the docstring for `test_func`."""
        pass

    test_func_wrapped = flonb.task_func()(test_func)

    assert test_func_wrapped.__name__ == "test_func"
    assert test_func_wrapped.__qualname__ == "test_signature.<locals>.test_func"
    assert inspect.signature(test_func_wrapped) == inspect.signature(test_func)
    assert test_func_wrapped.__doc__ == (
        f"This is a `flonb.Task` wrapping function {test_func}. "
        "Original function docstring:\n--------------\n\n"
        "This is the docstring for `test_func`."
    )
    assert test_func_wrapped.__module__ == "tests.test_task_func"


def test___repr__():
    @flonb.task_func()
    def dependency_func(d):
        pass

    @flonb.task_func()
    def test_func(a, b, c=flonb.Dep(dependency_func)):
        """This is the docstring for `test_func`."""
        pass

    expected_repr = (
        f"flonb.Task            test_func\n"
        f"func:                 {test_func.func}\n"
        f"cache_disk:           False\n"
        f"presupplied_options:  {{'b': 2}}\n"
        f"shallow option args:  ['a', 'b']\n"
        f"dependency args:      ['c']"
    )
    assert repr(test_func.partial(b=2)) == expected_repr


def test_Dep_repr():
    @flonb.task_func()
    def test_func(d):
        pass

    dep = flonb.Dep(test_func)
    assert repr(dep) == "flonb.Dep(test_func)"


def test_DynamicDep_repr():
    @flonb.task_func()
    def test_func(a):
        pass

    dep = flonb.DynamicDep(lambda cow, frog: test_func)
    assert repr(dep) == "flonb.DynamicDep(options=['cow', 'frog'])"
