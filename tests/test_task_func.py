import flonb


def test_callable_type():
    @flonb.task_func()
    def dummy_func():
        pass

    assert isinstance(dummy_func, flonb.Task)


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


def test_compute():
    @flonb.task_func()
    def add(x, y):
        return x + y

    assert add.compute(3, 4) == 7


def test_compute_with_dependency():
    @flonb.task_func()
    def compute_letter_count(word):
        return len(word)

    @flonb.task_func()
    def increase_letter_count(x, letter_count=compute_letter_count):
        return x + letter_count

    assert increase_letter_count.compute(x=5, word="cow") == 8


def test_compute_with_multiple_dependency():
    @flonb.task_func()
    def compute_letter_count(word):
        return len(word)

    @flonb.task_func()
    def compute_unique_letter_count(word):
        return len(set(word))

    @flonb.task_func()
    def total_score(
        unique_letter_count=compute_unique_letter_count,
        letter_count=compute_letter_count,
    ):
        return unique_letter_count + letter_count

    assert total_score.compute(word="banana") == 9


def test_compute_with_dependency_chain():
    @flonb.task_func()
    def compute_letter_count(word):
        return len(word)

    @flonb.task_func()
    def compute_unique_letter_count(word):
        return len(set(word))

    @flonb.task_func()
    def compute_total_score(
        unique_letter_count=compute_unique_letter_count,
        letter_count=compute_letter_count,
    ):
        return unique_letter_count + letter_count

    @flonb.task_func()
    def add_to_total_score(x, total_score=compute_total_score):
        return x + total_score

    assert add_to_total_score.compute(3, "banana") == 12
