TEMPLATE = {
    'project.py': """import signac
from flow import FlowProject
from flow import staticlabel

#def hello(job):
#    print("Hello, {{job}}".format(job=job))

class {project_class}(FlowProject):
    pass

    #@staticlabel()
    #def done(job):
    #    return job.isfile('hello.txt')
    #
    #def __init__(self, config=None):
    #    super({project_class}, self).__init__(config=config)
    #    # adding some operations
    #    self.add_operation( # the name of the operation
    #                        name='hello',
                             # A script that is submitted
    #                        cmd="python operations.py hello {{job}}",
                             # A list of functions that determine when this operation
                             # is 'ready', None means the operation is always ready.
    #                        prereqs = None,
                             # A list of functions that determine when this operation
                             # is 'finished', e.g. this operation is 'done' if 'hello.txt'
                             # exist in job workspace.
    #                        postconds = [
    #                          {project_class}.done
    #                        ]
    #                        )
    ## Enjoy.

if __name__ == '__main__':
    {project_class}().main()"""
}
