# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Status rendering logic."""
from .scheduling.base import JobStatus
from .util import mistune
from .util.misc import tqdm


def _render_status(
    template,
    template_environment,
    context,
    detailed,
    expand,
    unroll,
    compact,
    output_format,
):
    """Render the status.

    Parameters
    ----------
    template : str
        User provided Jinja2 template file.
    template_environment : :class:`jinja2.Environment`
        Template environment.
    context : dict
        Context that includes all the information for rendering status
        output.
    detailed : bool
        Print a detailed status of each job.
    expand : bool
        Present labels and operations in two separate tables.
    unroll : bool
        Separate columns for jobs and the corresponding operations.
    compact : bool
        Print a compact version of the output.
    output_format : str
        Rendering output format, supports:
        ``'terminal'`` (default), ``'markdown'``, or ``'html'``.

    Returns
    -------
    str
        Status output.

    """
    # Use Jinja2 template for status output
    if template is None:
        if detailed and expand:
            template = "status_expand.jinja"
        elif detailed and not unroll:
            template = "status_stack.jinja"
        elif detailed and compact:
            template = "status_compact.jinja"
        else:
            template = "status.jinja"

    def draw_progress_bar(value, total, escape="", width=40):
        """Visualize progress with a progress bar.

        Parameters
        ----------
        value : int
            The current progress as a fraction of total.
        total : int
            The maximum value that 'value' may obtain.
        width : int
            The character width of the drawn progress bar. (Default value = 40)
        escape : str
            Escape character needed for some formats. (Default value = "")

        Returns
        -------
        str
            Formatted progress bar.

        """
        assert value >= 0 and total > 0
        bar_format = (
            escape
            + f"|{{bar:{width}}}"
            + escape
            + "| {n_fmt}/{total_fmt} ({percentage:<0.2f}%)"
        )
        return tqdm.format_meter(n=value, total=total, elapsed=0, bar_format=bar_format)

    def job_filter(job_op, scheduler_status_code, all_ops):
        """Filter eligible jobs for status print.

        Parameters
        ----------
        job_op : dict
            Operation information for a job.
        scheduler_status_code : dict
            Dictionary information for status code.
        all_ops : bool
            Boolean value indicate if all operations should be displayed.

        Returns
        -------
        bool
            Whether the job is eligible to print.

        """
        return (
            scheduler_status_code[job_op["scheduler_status"]] != "U"
            or job_op["eligible"]
            or all_ops
        )

    def get_operation_status(operation_info, symbols):
        """Determine the status of an operation.

        Parameters
        ----------
        operation_info : dict
            Dictionary containing operation information.
        symbols : dict
            Dictionary containing code for different job statuses.

        Returns
        -------
        str
            The symbol for the job status.

        """
        if operation_info["scheduler_status"] >= JobStatus.active:
            op_status = "running"
        elif operation_info["scheduler_status"] > JobStatus.inactive:
            op_status = "active"
        elif operation_info["completed"]:
            op_status = "completed"
        elif operation_info["eligible"]:
            op_status = "eligible"
        else:
            op_status = "ineligible"

        return symbols[op_status]

    def highlight(string, eligible, pretty):
        """Change font to bold within jinja2 template.

        Parameters
        ----------
        string : str
            The string to be printed.
        eligible : bool
            Boolean value for job eligibility.
        pretty : bool
            Prettify the output.

        Returns
        -------
        str
            The highlighted (bold font) string.

        """
        if eligible and pretty:
            return "**" + string + "**"
        else:
            return string

    template_environment.filters["highlight"] = highlight
    template_environment.filters["draw_progress_bar"] = draw_progress_bar
    template_environment.filters["get_operation_status"] = get_operation_status
    template_environment.filters["job_filter"] = job_filter

    template = template_environment.get_template(template)
    markdown_output = template.render(**context)
    if output_format == "terminal":
        return mistune.terminal(markdown_output)
    elif output_format == "markdown":
        return markdown_output
    elif output_format == "html":
        return mistune.html(markdown_output)
    else:
        raise ValueError(
            "Output format not supported, valid options are "
            "terminal, markdown, or html."
        )
