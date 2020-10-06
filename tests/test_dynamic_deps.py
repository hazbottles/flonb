import flonb


def test_scaler_dep():
    @flonb.task_func()
    def add_y(x, y):
        return x + y

    @flonb.task_func()
    def multiply_by_y(x, y):
        return x * y

    @flonb.task_func()
    def exponify_by_z(
        z, base=lambda mode: {"add": add_y, "multiply": multiply_by_y}[mode]
    ):
        return base ** z

    assert exponify_by_z.compute(x=4, y=3, z=3, mode="add") == 343
    assert exponify_by_z.compute(x=4, y=3, z=2, mode="multiply") == 144


def test_list_deps():
    @flonb.task_func()
    def add_y(x, y):
        return x + y

    @flonb.task_func()
    def collect(
        container=lambda ys_range: [add_y.partial(y=y) for y in range(ys_range)],
    ):
        return container

    # Note how we don't specify `y`!
    assert collect.compute(x=3, ys_range=5) == [3, 4, 5, 6, 7]


def test_dict_deps():
    @flonb.task_func()
    def add_y(x, y):
        return x + y

    @flonb.task_func()
    def collect(
        container=lambda ys_range: {
            y + 10: add_y.partial(y=y) for y in range(ys_range)
        },
    ):
        return container

    # Note how we don't specify `y`!
    assert collect.compute(x=3, ys_range=5) == {13: 3, 14: 4, 15: 5, 16: 6, 17: 7}
