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
