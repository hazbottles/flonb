import inspect

import flonb


def test_signature():
    @flonb.task_func()
    def test_func(a, b, c=3):
        pass

    assert test_func.__name__ == "test_func"
    parameters = list(inspect.signature(test_func).parameters.items())
    assert parameters[0][0] == "a"
    raise NotImplementedError


def test_partial():
    # check signature
    # check calling
    raise NotImplementedError


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
