import flonb


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
    def replicate_word(x, letter_count=compute_letter_count):
        return letter_count * x

    assert replicate_word.compute(x=5, word="cow") == "cowcowcow"


def test_compute_with_dependency_chain():
    @flonb.task_func()
    def compute_letter_count(word):
        return len(word)

    @flonb.task_func()
    def compute_unique_letter_count(word):
        return len(set(word))

    @flonb.task_func()
    def compute_base_score(
        unique_letter_count=compute_unique_letter_count,
        letter_count=compute_letter_count,
    ):
        return unique_letter_count + letter_count

    @flonb.task_func()
    def increase_letter_count_score(
        letter_count_multiplier,
        letter_count=compute_letter_count,
        base_score=compute_base_score,
    ):
        return letter_count_multiplier * letter_count + base_score

    result = increase_letter_count_score.compute(
        letter_count_multiplier=3, word="banana"
    )
    assert result == 12
