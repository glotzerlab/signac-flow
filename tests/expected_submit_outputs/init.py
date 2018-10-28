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
        'xsede.CometEnvironment',
        'xsede.Stampede2Environment',
        'xsede.BridgesEnvironment',
        'umich.FluxEnvironment',
        'incite.TitanEnvironment',
        'incite.EosEnvironment'
        ]
    sps = cartesian(environment=environments)

    for sp in sps:
        project.open_job(sp).init()


if __name__ == "__main__":
    main()
