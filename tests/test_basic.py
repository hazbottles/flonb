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

    assert add.compute(x=3, y=4) == 7


def test_compute_with_dependency():
    @flonb.task_func()
    def compute_letter_count(word):
        return len(word)

    @flonb.task_func()
    def muliply_letter_count(x, letter_count=compute_letter_count):
        return letter_count * x

    assert muliply_letter_count.compute(x=5, word="cow") == 15


def test_compute_with_dependency_chain():
    @flonb.task_func()
    def compute_letter_count(word):
        return len(word)

    @flonb.task_func()
    def compute_unique_letter_count(word):
        return len(set(word))

    @flonb.task_func()
    def compute_n_magic_letter(word, magic_letter):
        n_magic_letter = 0
        for letter in word:
            if letter == magic_letter:
                n_magic_letter += 1
        return n_magic_letter

    @flonb.task_func()
    def compute_base_score(
        combining_mode,
        unique_letter_count=compute_unique_letter_count,
        letter_count=compute_letter_count,
        n_magic_letter=compute_n_magic_letter,
    ):
        if combining_mode == "add":
            return unique_letter_count + letter_count + n_magic_letter
        elif combining_mode == "multiply":
            return unique_letter_count * letter_count * n_magic_letter

    @flonb.task_func()
    def compute_total_score(
        bonus_multiplier,
        base_score=compute_base_score,
    ):
        return bonus_multiplier * base_score

    assert (
        compute_total_score.compute(
            bonus_multiplier=3, word="banana", magic_letter="n", combining_mode="add"
        )
        == (6 + 3 + 2) * 3
    )
    assert (
        compute_total_score.compute(
            bonus_multiplier=4,
            word="apple",
            magic_letter="p",
            combining_mode="multiply",
        )
        == (5 * 4 * 2) * 4
    )
