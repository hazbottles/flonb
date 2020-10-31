import pytest

import flonb


def test_nested_list_deps():
    @flonb.task_func()
    def multiply(x, y):
        return x * y

    @flonb.task_func()
    def collect(
        container=flonb.Dep(
            [[multiply.partial(x=x, y=y + 2) for x in range(3)] for y in range(2)]
        )
    ):
        return container

    assert collect.compute() == [
        [0 * 2, 1 * 2, 2 * 2],
        [0 * 3, 1 * 3, 2 * 3],
    ]


def test_heterogenous_deps():
    @flonb.task_func()
    def multiply(x, y):
        return x * y

    @flonb.task_func()
    def add(x, y):
        return x + y

    @flonb.task_func()
    def collect(
        container=flonb.Dep(
            [
                [{"cows": 4}, multiply.partial(x=2), [add.partial(x=2)]],
                5,
                add.partial(x=3),
                "here is a string",
            ]
        )
    ):
        return container

    assert collect.compute(y=4) == [
        [{"cows": 4}, 8, [6]],
        5,
        7,
        "here is a string",
    ]


@pytest.mark.xfail  # dicts are not parsed for tasks by dask - do we want to implement that on top?
def test_dict_deps():
    @flonb.task_func()
    def add_y(x, y):
        return x + y

    @flonb.task_func()
    def collect(
        container={y + 10: add_y.partial(y=y) for y in range(5)},
    ):
        return container

    # Note how we don't specify `y`!
    result = collect.compute(x=3)
    assert result == {13: 3, 14: 4, 15: 5, 16: 6, 17: 7}
