from flow import FlowProject


class TestProject(FlowProject):
    pass


@TestProject.label
def default_label(job):
    return True


@TestProject.label
def negative_default_label(job):
    return False


@TestProject.label
def b_is_even(job):
    return job.sp.b % 2 == 0


@TestProject.operation
@TestProject.cmd
@TestProject.pre(b_is_even)
@TestProject.post.isfile('world.txt')
def a_op(job):
    return 'echo "hello" > {job.ws}/world.txt'


if __name__ == '__main__':
    TestProject().main()
