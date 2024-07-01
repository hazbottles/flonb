import pytest
import flonb


def test_partial_options_do_not_propogate_outwards():
    @flonb.task_func()
    def add(x, y):
        return x + y

    @flonb.task_func()
    def add_to_3(base=flonb.Dep(add.partial(y=3))):
        return base

    assert add_to_3.graph_and_key(x=2)[1] == ("add_to_3", "x=2")  # no "y=3"!
    assert add_to_3.compute(x=2) == 5


def test_partial_option_supplied_that_is_used_deeper_in_chain():
    @flonb.task_func()
    def add(x, y):
        return x + y

    @flonb.task_func()
    def multiply(z, base=flonb.Dep(add)):
        return base * z

    assert multiply.partial(x=3).compute(y=2, z=4) == 20


def test_multiple_applications_of_partial():

    @flonb.task_func
    def add(a, b):
        return a + b

    assert 5 == add.partial(a=3).partial(b=2).compute()


def test_multiple_applications_of_partial_raises():

    @flonb.task_func
    def add_one(a):
        return a + 1

    with pytest.raises(ValueError) as excinfo:
        add_one.partial(a=3).partial(a=3)
    assert (
        str(excinfo.value)
        == "Options ['a'] have already been pre-supplied to 'add_one'."
    )


def test_partial_deep_in_graph():

    @flonb.task_func
    def add(a, b):
        return a + b

    @flonb.task_func
    def multiply(a, b):
        return a * b

    @flonb.task_func
    def my_calc(
        added=flonb.Dep(add),
        # multiply should ignore the (a,b) supplied to add
        multiplied=flonb.DynamicDep(lambda x, y: multiply.partial(a=x, b=y)),
    ):
        return added, multiplied

    assert (3, 20) == my_calc.compute(a=1, b=2, x=4, y=5)


def test_partial_and_compute():
    @flonb.task_func
    def my_task(a):
        return a + 1

    with pytest.raises(ValueError) as excinfo:
        my_task.partial(a=1).compute(a=2)
    assert "Excess options supplied: ['a']." == str(excinfo.value)
