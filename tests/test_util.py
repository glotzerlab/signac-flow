from flow.util.misc import bidict


class TestBidict:
    def test_bidict(self):
        # The bidict class inherits from dict, so we trust its inherited
        # methods and only need to validate its overrides and inverse behavior.
        bd = bidict({"a": 1, "b": 2})
        bd["c"] = 1
        assert bd == {"a": 1, "c": 1, "b": 2}
        assert bd.inverse == {1: ["a", "c"], 2: ["b"]}
        del bd["c"]
        assert bd == {"a": 1, "b": 2}
        assert bd.inverse == {1: ["a"], 2: ["b"]}
        del bd["a"]
        assert bd == {"b": 2}
        assert bd.inverse == {2: ["b"]}
        bd["b"] = 3
        assert bd == {"b": 3}
        assert bd.inverse == {3: ["b"]}
