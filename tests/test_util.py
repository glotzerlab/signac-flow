import pytest

from flow.util.misc import _bidict
from flow.util.template_filters import calc_memory


@pytest.fixture()
def mock_operations():
    class MockOp:
        def __init__(self, memory=None):
            self.directives = {"memory": memory}

    return [MockOp(1), MockOp(8), MockOp()]


class TestBidict:

    bd = _bidict({"a": 1, "b": 2})

    def add_c_bidict(self):
        self.bd["c"] = 1

    def test_basic_dict(self):
        self.add_c_bidict()
        assert self.bd == {"a": 1, "c": 1, "b": 2}
        assert self.bd.inverse == {1: ["a", "c"], 2: ["b"]}
        assert len(self.bd) == 3
        assert list(self.bd.keys()) == ["a", "b", "c"]
        assert list(self.bd.values()) == [1, 2, 1]
        assert list(self.bd.items()) == [("a", 1), ("b", 2), ("c", 1)]

    def test_get_func(self):
        assert self.bd.get("a") == 1
        assert self.bd.get("q", 42) == 42

    def test_belonging_func(self):
        assert "a" in self.bd
        assert "q" not in self.bd

    def test_del_c(self):
        self.add_c_bidict()
        del self.bd["c"]
        assert self.bd == {"a": 1, "b": 2}
        assert self.bd.inverse == {1: ["a"], 2: ["b"]}
        assert len(self.bd) == 2

    def test_del_a(self):
        del self.bd["a"]
        assert self.bd == {"b": 2}
        assert self.bd.inverse == {2: ["b"]}

    def test_add_b(self):
        self.bd["b"] = 3
        assert self.bd == {"b": 3}
        assert self.bd.inverse == {3: ["b"]}

    def test_update(self):
        self.bd.update({"c": 2})
        assert self.bd == {"b": 3, "c": 2}
        assert self.bd.inverse == {2: ["c"], 3: ["b"]}

    def test_popitem(self):
        # Note that MutableMapping's popitem is *not* LIFO-ordered, it pops
        # according to the key iteration order (which means it defaults to FIFO
        # for the underlying dict, which is insertion-ordered).
        assert self.bd.popitem() == ("b", 3)
        assert self.bd == {"c": 2}
        assert self.bd.inverse == {2: ["c"]}

    def test_set_default_overlap(self):
        self.bd.setdefault("c", 7)
        assert self.bd == {"c": 2}
        assert self.bd.inverse == {2: ["c"]}

    def test_default_no_overlap(self):
        self.bd.setdefault("a", 9)
        assert self.bd == {"c": 2, "a": 9}
        assert self.bd.inverse == {2: ["c"], 9: ["a"]}

    def test_pop(self):
        assert self.bd.pop("c") == 2
        assert self.bd == {"a": 9}
        assert self.bd.inverse == {9: ["a"]}

    def test_clear(self):
        self.bd.clear()
        assert self.bd == {}
        assert self.bd.inverse == {}


class TestTemplateFilters:
    def test_calc_memory(self, mock_operations):
        op1 = mock_operations[0]
        op2 = mock_operations[1]
        op3 = mock_operations[2]

        # Test when operations run in serial
        assert calc_memory([op1], False) == 1
        assert calc_memory([op1, op2, op3], False) == 8

        # Test when operations run in parallel
        assert calc_memory([op1, op2], True) == 9
        assert calc_memory([op3], True) == 0
        assert calc_memory([op1, op2, op3], True) == 9
