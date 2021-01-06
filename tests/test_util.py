from flow.util.misc import _bidict


class TestBidict:
    def test_bidict(self):
        # The bidict class inherits from dict, so we trust its inherited
        # methods and only need to validate its overrides and inverse behavior.
        bd = _bidict({"a": 1, "b": 2})
        bd["c"] = 1
        assert bd == {"a": 1, "c": 1, "b": 2}
        assert bd.inverse == {1: ["a", "c"], 2: ["b"]}
        assert len(bd) == 3
        assert list(bd.keys()) == ["a", "b", "c"]
        assert list(bd.values()) == [1, 2, 1]
        assert list(bd.items()) == [("a", 1), ("b", 2), ("c", 1)]
        assert bd.get("a") == 1
        assert bd.get("q", 42) == 42
        assert "a" in bd
        assert "q" not in bd
        del bd["c"]
        assert bd == {"a": 1, "b": 2}
        assert bd.inverse == {1: ["a"], 2: ["b"]}
        assert len(bd) == 2
        del bd["a"]
        assert bd == {"b": 2}
        assert bd.inverse == {2: ["b"]}
        bd["b"] = 3
        assert bd == {"b": 3}
        assert bd.inverse == {3: ["b"]}
        bd.update({"c": 2})
        assert bd == {"b": 3, "c": 2}
        assert bd.inverse == {2: ["c"], 3: ["b"]}
        # Note that MutableMapping's popitem is *not* LIFO-ordered, it pops
        # according to the key iteration order (which means it defaults to FIFO
        # for the underlying dict, which is insertion-ordered).
        assert bd.popitem() == ("b", 3)
        assert bd == {"c": 2}
        assert bd.inverse == {2: ["c"]}
        bd.setdefault("c", 7)
        assert bd == {"c": 2}
        assert bd.inverse == {2: ["c"]}
        bd.setdefault("a", 9)
        assert bd == {"c": 2, "a": 9}
        assert bd.inverse == {2: ["c"], 9: ["a"]}
        assert bd.pop("c") == 2
        assert bd == {"a": 9}
        assert bd.inverse == {9: ["a"]}
        bd.clear()
        assert bd == {}
        assert bd.inverse == {}
