# make separate python class for render_status
import mistune
from .scheduling.base import JobStatus


class _render_status:

    def __init__(self):
        self.markdown_output = None
        self.terminal_output = None
        self.html_output = None

    def generate_markdown_output(self, template, context):
        self.markdown_output = template.render(**context)

    def generate_terminal_output(self, template, context):
        self.terminal_output = template.render(**context)

    def generate_html_output(self, template, context):
        self.generate_markdown_output(template, context)
        self.html_output = mistune.markdown(self.markdown_output)

    def render(self, template, template_environment, context, file, detailed, expand,
               unroll, compact, pretty, option):

        # use Jinja2 template for status output
        if option == 'terminal':
            prefix = 'terminal_'
        else:
            prefix = 'md_'
        if template is None:
            if detailed and expand:
                template = prefix + 'status_expand.jinja'
            elif detailed and not unroll:
                template = prefix + 'status_stack.jinja'
            elif detailed and compact:
                template = prefix + 'status_compact.jinja'
            else:
                template = prefix + 'status.jinja'

        def draw_progressbar(value, total, escape='', width=40):
            """Visualize progess with a progress bar.

            :param value:
                The current progress as a fraction of total.
            :type value:
                int
            :param total:
                The maximum value that 'value' may obtain.
            :type total:
                int
            :param width:
                The character width of the drawn progress bar.
            :type width:
                int
            """

            assert value >= 0 and total > 0
            ratio = ' %0.2f%%' % (100 * value / total)
            n = int(value / total * width)
            return escape + '|' + ''.join(['#'] * n) + ''.join(['-'] * (width - n)) \
                          + escape + '|' + ratio

        def job_filter(job_op, scheduler_status_code, all_ops):
            """filter eligible jobs for status print.

            :param job_ops:
                Operations information for a job.
            :type job_ops:
                OrderedDict
            :param scheduler_status_code:
                Dictionary information for status code
            :type scheduler_status_code:
                Dictionary
            :param all_ops:
                Boolean value indicate if all operations should be displayed
            :type all_ops:
                Boolean
            """

            if scheduler_status_code[job_op['scheduler_status']] != 'U' or \
               job_op['eligible'] or all_ops:
                return True
            else:
                return False

        def get_operation_status(operation_info, symbols):
            """Determine the status of an operation.

            :param operation_info:
                Dicionary containing operation information
            :type operation_info:
                Dictionary
            :param symbols:
                Dicionary containing code for different job status
            :type symbols:
                Dictionary
            """

            if operation_info['scheduler_status'] >= JobStatus.active:
                op_status = u'running'
            elif operation_info['scheduler_status'] > JobStatus.inactive:
                op_status = u'active'
            elif operation_info['completed']:
                op_status = u'completed'
            elif operation_info['eligible']:
                op_status = u'eligible'
            else:
                op_status = u'ineligible'

            return symbols[op_status]

        if pretty:
            def highlight(s, eligible, prefix_str, suffix_str):
                """Change font to bold within jinja2 template

                :param s:
                    The string to be printed
                :type s:
                    str
                :param eligible:
                    Boolean value for job eligibility
                :type eligible:
                    Boolean
                """
                if eligible:
                    return prefix_str + s + suffix_str
                else:
                    return s
        else:
            def highlight(s, eligible, prefix_str, suffix_str):
                """Change font to bold within jinja2 template

                :param s:
                    The string to be printed
                :type s:
                    str
                :param eligible:
                    Boolean value for job eligibility
                :type eligible:
                    boolean
                """
                return s

        template_environment.filters['highlight'] = highlight
        template_environment.filters['draw_progressbar'] = draw_progressbar
        template_environment.filters['get_operation_status'] = get_operation_status
        template_environment.filters['job_filter'] = job_filter

        template = template_environment.get_template(template)

        if option == 'terminal':
            self.generate_terminal_output(template, context)
            print(self.terminal_output, file=file)
            return self.terminal_output
        elif option == 'html':
            self.generate_html_output(template, context)
            print(self.html_output, file=file)
            return self.html_output
        elif option == 'md' or 'markdown':
            self.generate_markdown_output(template, context)
            print(self.markdown_output, file=file)
            return self.markdown_output
