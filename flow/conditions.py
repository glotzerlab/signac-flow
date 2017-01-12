class And:

    def __init__(self, * conditions):
        self.conditions = conditions

    def __call__(self, job):
        return all(c(job) for c in self.conditions)

class Or:

    def __init__(self, * conditions):
        self.conditions = conditions

    def __call__(self):
        return any(c(job) for c in self.conditions)


class IsFile:

    def __init__(self, fn):
        self.fn = fn

    def __call__(self, job):
        return job.isfile(self.fn)

    def __repr__(self):
        return 'IsFile({})'.format(self.fn)
