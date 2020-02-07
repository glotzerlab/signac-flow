# make separate python class for render_status
import mistune
from .scheduling.base import JobStatus


class renderer:
    """A class for rendering status in different format.

    This class provides method and string output for rendering status output in different format,
    currently supports: terminal, markdown and html

    """

    def __init__(self):
        self.markdown_output = None
        self.terminal_output = None
        self.html_output = None

    def generate_terminal_output(self):
        self.terminal_output = mistune.terminal(self.markdown_output)

    def generate_html_output(self):
        self.html_output = mistune.html(self.markdown_output)

    def render(self, template, template_environment, context, file, detailed, expand,
               unroll, compact, pretty, option):
        """Render the status in different format for print_status.

        :param template:
            User provided Jinja2 template file.
        :type template:
            str
        :param template_environment:
            template environment
        :type template_environment:
            :class:'FlowProject._template_environment'
        :param context:
            Context that includes all the information for rendering status output.
        :type context:
            dict
        :param file:
            Redirect all output to this file, defaults to sys.stdout.
        :type file:
            str
        :param detailed:
            Print a detailed status of each job.
        :type detailed:
            bool
        :param expand:
            Present labels and operations in two separate tables.
        :type expand:
            bool
        :param unroll:
            Separate columns for jobs and the corresponding operations.
        :type unroll:
            bool
        :param compact:
            Print a compact version of the output.
        :type compact:
            bool
        :param pretty:
            Prettify the output.
        :type pretty:
            bool
        :param option:
            Options for rendering format, currently supports:
            'terminal'(default), 'markdown' and 'html'.
        :type option:
            str
        """

        # use Jinja2 template for status output
        if template is None:
            if detailed and expand:
                template = 'status_expand.jinja'
            elif detailed and not unroll:
                template = 'status_stack.jinja'
            elif detailed and compact:
                template = 'status_compact.jinja'
            else:
                template = 'status.jinja'

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
                dict
            :param all_ops:
                Boolean value indicate if all operations should be displayed
            :type all_ops:
                bool
            """

            if scheduler_status_code[job_op['scheduler_status']] != 'U' or \
               job_op['eligible'] or all_ops:
                return True
            else:
                return False

        def get_operation_status(operation_info, symbols):
            """Determine the status of an operation.

            :param operation_info:
                Dictionary containing operation information
            :type operation_info:
                dict
            :param symbols:
                Dictionary containing code for different job status
            :type symbols:
                dict
            """

            if operation_info['scheduler_status'] >= JobStatus.active:
                op_status = 'running'
            elif operation_info['scheduler_status'] > JobStatus.inactive:
                op_status = 'active'
            elif operation_info['completed']:
                op_status = 'completed'
            elif operation_info['eligible']:
                op_status = 'eligible'
            else:
                op_status = 'ineligible'

            return symbols[op_status]

        if pretty:
            def highlight(s, eligible, prefix_str='**', suffix_str='**'):
                """Change font to bold within jinja2 template

                :param s:
                    The string to be printed
                :type s:
                    str
                :param eligible:
                    Boolean value for job eligibility
                :type eligible:
                    bool
                :param prefix_str:
                    String prefix for bold syntax
                :type prefix_str:
                    str
                :param suffix_str:
                    String prefix for bold syntax
                :type suffix_str:
                    str
                """
                if eligible:
                    return prefix_str + s + suffix_str
                else:
                    return s
        else:
            def highlight(s, eligible, prefix_str='**', suffix_str='**'):
                """Change font to bold within jinja2 template

                :param s:
                    The string to be printed
                :type s:
                    str
                :param eligible:
                    Boolean value for job eligibility
                :type eligible:
                    bool
                :param prefix_str:
                    String prefix for bold syntax
                :type prefix_str:
                    str
                :param suffix_str:
                    String prefix for bold syntax
                :type suffix_str:
                    str
                """
                return s

        template_environment.filters['highlight'] = highlight
        template_environment.filters['draw_progressbar'] = draw_progressbar
        template_environment.filters['get_operation_status'] = get_operation_status
        template_environment.filters['job_filter'] = job_filter

        template = template_environment.get_template(template)
        self.markdown_output = template.render(**context)
        if option == 'terminal':
            self.generate_terminal_output()
            return self.terminal_output
        elif option == 'html':
            self.generate_html_output()
            return self.html_output
        elif option == 'md' or option == 'markdown':
            return self.markdown_output
        else:
            raise ValueError('Option not supported, valid option format:'
                             'termial, html and markdown/md')
