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
    def add(y, x=str_to_int):
        return x + y

    assert add(2, 4) == 6


def test_signature():
    def test_func(a, b, c=3):
        """This is a docstring"""
        pass

    test_func_wrapped = flonb.task_func(test_func)

    assert test_func_wrapped.__name__ == "test_func"
    assert test_func_wrapped.__qualname__ == "test_func"
    assert inspect.signature(test_func_wrapped) == inspect.signature(test_func)
    assert test_func.__doc__ == f"This is a `flonb.Task`.  test_func_wrapped.__doc__"


import functools

functools.update_wrapper


def test___repr__(self):
    raise NotImplementedError


def test_missing_deep_option_raises_error():
    raise NotImplementedError


def test_missing_option_for_dynamic_dep():
    raise NotImplementedError


def test_superflous_option_raises_error():
    raise NotImplementedError


def test_supplied_options_collision_with_partial_options():
    raise NotImplementedError


def test_task_func_raises_error_in_compute():
    raise NotImplementedError
