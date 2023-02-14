import pytest

from flow.util.misc import _bidict
from flow.util.template_filters import calc_memory


class TestBidict:
    @pytest.fixture()
    def bd(self):
        return _bidict({"a": 1, "b": 2})

    def test_basic_dict(self, bd):
        bd["c"] = 1
        assert bd == {"a": 1, "c": 1, "b": 2}
        assert bd.inverse == {1: ["a", "c"], 2: ["b"]}
        assert len(bd) == 3
        assert list(bd.keys()) == ["a", "b", "c"]
        assert list(bd.values()) == [1, 2, 1]
        assert list(bd.items()) == [("a", 1), ("b", 2), ("c", 1)]

    def test_get_func(self, bd):
        assert bd.get("a") == 1
        assert bd.get("q", 42) == 42

    def test_contains(self, bd):
        assert "a" in bd
        assert "q" not in bd

    def test_delitem(self, bd):
        bd["c"] = 1
        del bd["c"]
        assert bd == {"a": 1, "b": 2}
        assert bd.inverse == {1: ["a"], 2: ["b"]}
        assert len(bd) == 2

    def test_setitem(self, bd):
        bd["b"] = 3
        assert bd == {"a": 1, "b": 3}
        assert bd.inverse == {3: ["b"], 1: ["a"]}

    def test_update(self, bd):
        bd.update({"c": 2})
        assert bd == {"b": 2, "c": 2, "a": 1}
        assert bd.inverse == {1: ["a"], 2: ["b", "c"]}

    def test_popitem(self, bd):
        # Note that MutableMapping's popitem is *not* LIFO-ordered, it pops
        # according to the key iteration order (which means it defaults to FIFO
        # for the underlying dict, which is insertion-ordered).
        assert bd.popitem() == ("a", 1)
        assert bd == {"b": 2}
        assert bd.inverse == {2: ["b"]}

    def test_setdefault_existing_key(self, bd):
        bd.setdefault("b", 7)
        assert bd == {"b": 2, "a": 1}
        assert bd.inverse == {1: ["a"], 2: ["b"]}

    def test_setdefault_nonexistant_key(self, bd):
        bd.setdefault("c", 9)
        assert bd == {"a": 1, "b": 2, "c": 9}
        assert bd.inverse == {2: ["b"], 9: ["c"], 1: ["a"]}

    def test_pop(self, bd):
        assert bd.pop("b") == 2
        assert bd == {"a": 1}
        assert bd.inverse == {1: ["a"]}

    def test_clear(self, bd):
        bd.clear()
        assert bd == {}
        assert bd.inverse == {}


class TestTemplateFilters:
    @pytest.fixture()
    def mock_operations(self):
        class MockOp:
            def __init__(self, memory=None):
                self.directives = {"memory": memory}

        return [MockOp(1), MockOp(8), MockOp()]

    def test_calc_memory_serial(self, mock_operations):
        # Test when operations run in serial
        assert calc_memory([mock_operations[0]], False) == 1
        assert (
            calc_memory(
                [mock_operations[0], mock_operations[1], mock_operations[2]], False
            )
            == 8
        )

    def test_calc_memory_parallel(self, mock_operations):
        # Test when operations run in parallel
        assert calc_memory([mock_operations[0], mock_operations[1]], True) == 9
        assert calc_memory([mock_operations[2]], True) == 0
        assert (
            calc_memory(
                [mock_operations[0], mock_operations[1], mock_operations[2]], True
            )
            == 9
        )
