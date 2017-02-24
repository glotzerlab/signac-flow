import unittest

from flow.graph import FlowOperation
from flow.graph import FlowCondition
from flow.graph import FlowGraph


class FlowOperationTest(unittest.TestCase):

    def test_flow_operation_with_callable(self):
        foo = [False]

        def bar(foo):
            foo[0] = True

        op = FlowOperation(bar)
        op(foo)
        self.assertTrue(foo[0])

    def test_flow_operation_hashing(self):

        def foo(_):
            return True

        for x in (None, 'test', foo, lambda _: 0):
            op1 = FlowOperation(x)
            op2 = FlowOperation(x)
            self.assertEqual(op1, op2)

        self.assertEqual(str(FlowOperation('test')), 'test')
        self.assertEqual(str(FlowOperation(foo)), 'foo')


class FlowConditionTest(unittest.TestCase):

    def test_flow_condition_init(self):
        for x in ('test', 0):
            with self.assertRaises(ValueError):
                FlowCondition(x)

        def foo(_):
            return True

        for x in (None, foo, lambda _: True):
            FlowCondition(x)

    def test_flow_condition_hashing(self):

        def foo(_):
            return True

        for x in (None, foo, lambda _: True):
            cond1 = FlowCondition(x)
            cond2 = FlowCondition(x)
            self.assertEqual(cond1, cond2)

    def test_flow_condition_str(self):

        def foo(_):
            return True

        self.assertEqual(str(FlowCondition(foo)), 'foo')

    def test_flow_condition_call(self):
        with self.assertRaises(TypeError):
            FlowCondition()

        with self.assertRaises(TypeError):
            self.assertTrue(FlowCondition()())

        self.assertTrue(FlowCondition(None)(None))

        foo = [False]

        def bar(cond):
            foo[0] = True

        FlowCondition(bar)(None)
        self.assertTrue(foo[0])


class FlowGraphTest(unittest.TestCase):

    def test_add_operation(self):
        g = FlowGraph()
        op = FlowOperation('test')
        g.add_operation(op)
        for op_ in g.operations():
            self.assertEqual(op, op_)

        g = FlowGraph()
        g.add_operation('test')
        for op_ in g.operations():
            self.assertEqual(op, op_)

    def test_pre_post_conds(self):

        def false(_):
            return False

        g = FlowGraph()
        op = FlowOperation('test')
        g.add_operation(op, prereq=None, postconds=[false])
        self.assertTrue(g.eligible(op, None))
        self.assertEqual(len(list(g.operations())), 1)
        self.assertEqual(len(list(g.eligible_operations(None))), 1)

        g = FlowGraph()
        g.add_operation('test', prereq=None, postconds=[false])
        self.assertEqual(len(list(g.operations())), 1)
        self.assertEqual(len(list(g.eligible_operations(None))), 1)

        foo = [False]

        def bar(_):
            return foo[0]

        g = FlowGraph()
        c = FlowCondition(bar)
        g.add_operation('test', prereq=c, postconds=[false])
        self.assertEqual(len(list(g.operations())), 1)
        self.assertEqual(len(list(g.eligible_operations(None))), 0)
        foo[0] = True
        self.assertEqual(len(list(g.eligible_operations(None))), 1)
        foo[0] = False
        self.assertEqual(len(list(g.eligible_operations(None))), 0)

        g = FlowGraph()
        c = FlowCondition(bar)
        g.add_operation('test', prereq=bar, postconds=[false])
        self.assertEqual(len(list(g.operations())), 1)
        self.assertEqual(len(list(g.eligible_operations(None))), 0)
        foo[0] = True
        self.assertEqual(len(list(g.eligible_operations(None))), 1)
        foo[0] = False
        self.assertEqual(len(list(g.eligible_operations(None))), 0)

    def test_operation_chain(self):
        foo = [False]

        def bar(_):
            return foo[0]

        def false(_):
            return False

        g = FlowGraph()
        g.add_operation('test', prereq=bar, postconds=[false])
        with self.assertRaises(ValueError):
            list(g.operation_chain(None, 'test'))
        with self.assertRaises(ValueError):
            list(g.operation_chain(None, false, start='test'))
        with self.assertRaises(RuntimeError):
            list(g.operation_chain(None, bar, start=false))

        chain = list(g.operation_chain(None, false, start=bar))
        self.assertEqual(len(chain), 1)
        self.assertEqual(chain[0], FlowOperation('test'))

    def test_link_conditions(self):
        foo = [False]

        def bar(_):
            return foo[0]

        def bar_(_):
            return foo[0]

        def false(_):
            return False

        g = FlowGraph()
        g.add_operation('foo', prereq=None, postconds=[bar])
        g.add_operation('foo2', prereq=bar_, postconds=[false])
        self.assertEqual(len(list(g.operation_chain(None, bar))), 1)
        self.assertEqual(len(list(g.operation_chain(None, false, bar_))), 1)

        with self.assertRaises(RuntimeError):
            list(g.operation_chain(None, false))

        g.link_conditions(bar, bar_)
        self.assertEqual(len(list(g.operation_chain(None, false))), 2)

        self.assertEqual(len(list(g.operation_chain(None, bar))), 1)
        self.assertEqual(len(list(g.operation_chain(None, false, bar_))), 1)


if __name__ == '__main__':
    unittest.main()
