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
        z,
        base=flonb.DynamicDep(
            lambda mode: {"add": add_y, "multiply": multiply_by_y}[mode]
        ),
    ):
        return base**z

    assert exponify_by_z.compute(x=4, y=3, z=3, mode="add") == 343
    assert exponify_by_z.compute(x=4, y=3, z=2, mode="multiply") == 144


def test_list_deps():
    @flonb.task_func()
    def add_y(x, y):
        return x + y

    @flonb.task_func()
    def collect(
        container=flonb.DynamicDep(
            lambda ys_range: ([add_y.partial(y=y) for y in range(ys_range)])
        ),
    ):
        return container

    assert collect.compute(x=3, ys_range=5) == [3, 4, 5, 6, 7]


@flonb.task_func
def test_dict_deps():
    @flonb.task_func
    def add_one(a):
        return a + 1

    @flonb.task_func
    def my_task2(res=flonb.Dep({"add_one": add_one})):
        return res

    assert {"a": 3} == my_task2.compute(a=2)
