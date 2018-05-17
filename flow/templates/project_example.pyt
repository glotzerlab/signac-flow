from flow import FlowProject


class {{ project_class_name }}(FlowProject):
    pass


@{{ project_class_name }}.label
def said_hello(job):
    return job.document.get('hello', False)


@{{ project_class_name }}.operation
@{{ project_class_name }}.post(said_hello)
def say_hello(job):
    print("Hello,", job, "!")
    job.document.hello = True


if __name__ == '__main__':
    {{ project_class_name }}().main()
