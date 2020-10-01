import flonb


def test_task_func_type():
    @flonb.task_func()
    def dummy_func():
        pass

    assert isinstance(dummy_func, flonb.Task)


def test_task_func_callable():
    @flonb.task_func()
    def add(x, y):
        return x + y

    assert add(1, 4) == 5
