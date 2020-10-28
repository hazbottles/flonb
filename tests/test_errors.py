import pytest

import flonb


@flonb.task_func()
def parent(a, b):
    pass


@flonb.task_func()
def child(c, adult=parent):
    pass


def test_missing_option_raises_error():
    with pytest.raises(ValueError) as excinfo:
        parent.compute(a=4)
    assert "Missing option 'b'." in str(excinfo)


def test_missing_deep_option_raises_error():
    with pytest.raises(ValueError) as excinfo:
        child.compute(a=4, c=3)
    assert "Missing option 'b'." in str(excinfo)


def test_superflous_option_raises_error():
    with pytest.raises(ValueError) as excinfo:
        child.compute(a=4, b=3, c=2, d=3, e=0)
    assert "Excess options supplied: ['d', 'e']." in str(excinfo)


def test_dependency_supplied_to_compute_raises_error():
    with pytest.raises(ValueError) as excinfo:
        child.compute(a=4, b=3, c=2, adult=3)
    assert "Excess options supplied: ['adult']." in str(excinfo)


def test_supplied_options_collision_with_partial_options():
    with pytest.raises(ValueError) as excinfo:
        parent.partial(a=2).compute(a=2, b=4)
    assert "Options ['a'] have already been pre-supplied to 'parent'." in str(excinfo)


def test_bad_partial_option():
    with pytest.raises(ValueError) as excinfo:
        child.partial(a=2).compute(b=2, c=3)
    assert "Pre-supplied option 'a'=2 is not a valid option for 'child'." in str(
        excinfo
    )


def test_missing_option_for_dynamic_dep():
    @flonb.task_func()
    def dynamic(person=lambda mode: child if mode == "child" else parent):
        pass

    with pytest.raises(ValueError) as excinfo:
        dynamic.compute(a=4, b=1, c=3)
    assert "Missing option 'mode'." in str(excinfo)


def test_task_func_raises_error_in_compute():
    class MyError(Exception):
        pass

    @flonb.task_func()
    def raises_error():
        raise MyError

    @flonb.task_func()
    def test_func(dep=raises_error):
        pass

    with pytest.raises(MyError):
        test_func.compute()
