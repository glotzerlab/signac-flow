import signac
import flow
import flow.environments
import itertools

def cartesian(**kwargs):
    for combo in itertools.product(*kwargs.values()):
        yield dict(zip(kwargs.keys(), combo))

def main():
    project = signac.init_project('SubmissionTest')
    environments = [
        'environment.UnknownEnvironment',
        'environments.xsede.CometEnvironment',
        'environments.xsede.Stampede2Environment',
        'environments.xsede.BridgesEnvironment',
        'environments.umich.FluxEnvironment',
        'environments.incite.TitanEnvironment',
        'environments.incite.EosEnvironment'
        ]
    sps = cartesian(environment=environments)

    for sp in sps:
        project.open_job(sp).init()


if __name__ == "__main__":
    main()
