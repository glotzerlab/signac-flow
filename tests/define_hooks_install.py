from define_hooks_test_project import (
    _HooksTestProject,
    set_job_doc,
    set_job_doc_with_error,
)


class ProjectLevelHooks:
    keys = (
        "installed_start",
        "installed_finish",
        "installed_success",
        "installed_fail",
    )

    def install_hooks(self, project):
        project.hooks.on_start.append(set_job_doc(self.keys[0]))
        project.hooks.on_finish.append(set_job_doc(self.keys[1]))
        project.hooks.on_success.append(set_job_doc(self.keys[2]))
        project.hooks.on_fail.append(set_job_doc_with_error(self.keys[3]))
        return project

    __call__ = install_hooks


if __name__ == "__main__":
    ProjectLevelHooks().install_hooks(_HooksTestProject()).main()
