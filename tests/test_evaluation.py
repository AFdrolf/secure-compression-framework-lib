from evaluation.util import generate_distribution


def test_evaluation_dist_generation():
    assert generate_distribution(10, 100, "even") == [10] * 10
    assert generate_distribution(10, 101, "even") == [11] + [10] * 9
    assert generate_distribution(10, 102, "even") == [11, 11] + [10] * 8

    assert generate_distribution(10, 100, "long_tail")[0] == 80
    assert sum(generate_distribution(10, 100, "long_tail")[1:]) == 20
    assert sum(generate_distribution(20, 100, "long_tail")[:2]) == 80
    assert sum(generate_distribution(20, 100, "long_tail")[2:]) == 20
    assert generate_distribution(2, 100, "long_tail") == [80, 20]

    assert sum(generate_distribution(10, 100, "random")) == 100
    assert len(set(generate_distribution(10, 100, "random"))) > 1
