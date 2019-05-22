import signac

project = signac.init_project('group-project')

for p in range(1, 10):
    sp = {'num': p}
    job = project.open_job(sp)
    job.init()
