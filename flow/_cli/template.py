"Example files that may be used for faster initialization."

TEMPLATES = {


# minimal templates:
    'minimal': {
        '{alias}.py': """from flow import FlowProject


class {project_class}(FlowProject):
    pass


if __name__ == '__main__':
    {project_class}().main()""",
    },
# end of minimal

# example:
    'example': {
        '{alias}.py': """from flow import FlowProject
from flow import JobOperation
from flow import staticlabel


class {project_class}(FlowProject):

    @staticlabel()
    def greeted(job):
        return job.isfile('hello.txt')

    def next_operation(self, job):
        if not self.greeted(job):
            return JobOperation(
                # The name of the operation (may be freely choosen)
                'hello',

                # A reference to the job that this operation operates on
                job,

                # The command (/script) to be executed for this operation
                cmd='python operations.py hello {{job}}')


if __name__ == '__main__':
    {project_class}().main()""",

    'operations.py': """def hello(job):
    print("Hello", job)
    with job:
        with open('hello.txt', 'w') as f:
            f.write('world!')


if __name__ == '__main__':
    import flow
    flow.run()"""
    },
# end of example


# example conditions:
    'example-conditions': {
        '{alias}.py': """from flow import FlowProject
from flow import staticlabel


class {project_class}(FlowProject):

    @staticlabel()
    def greeted(job):
        return job.isfile('hello.txt')

    def __init__(self, config=None):
        super({project_class}, self).__init__(config=config)

        # Add hello world operation
        self.add_operation(

            # The name of the operation (may be freely choosen)
            name='hello',

            # The command (/script) to be executed for this operation
            cmd='python operations.py hello {{job}}',

            # A list of functions that determine wether this operation is 'ready',
            # a value of None means that the operation is always ready.
            prereqs=None,

            # A list of functions that determine, when this operation is 'finished', e.g.,
            # this operation is 'done', if the file 'hello.txt' exists in the job's workspace.
            postconds = [{project_class}.greeted]
            )

if __name__ == '__main__':
    {project_class}().main()""",

    'operations.py': """def hello(job):
    print("Hello", job)
    with job:
        with open('hello.txt', 'w') as f:
            f.write('world!')


if __name__ == '__main__':
    import flow
    flow.run()"""
    }
# end of example-conditions

}
