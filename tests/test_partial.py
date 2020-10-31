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
