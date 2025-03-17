import os
import logging
import signal
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir
from random import choice

from airflow.hooks.base import BaseHook

from airflow.exceptions import AirflowException


class BashHook(BaseHook):
    conn_name_attr = 'conn_id'
    default_conn_name = 'bash_default'

    def __init__(self, *args, **kwargs):
        super().__init__()

        if not self.conn_name_attr:
            raise AirflowException("conn_name_attr is not defined")
        elif len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])

        self.__cached_connection = None

    def render_command(self, bash_command, hide_password=False, *args, **kwargs):
        result = bash_command
        if self.conn_id is not None:
            if not self.__cached_connection:
                base_conn = self.get_connection(self.conn_id)
                self.__cached_connection = base_conn
            else:
                base_conn = self.__cached_connection
            render_dict = base_conn.__dict__
            render_dict['password'] = base_conn.password
            render_dict = {**render_dict, **base_conn.extra_dejson, **kwargs}
            if render_dict.get('host', '').count(',') > 0:
                render_dict['host'] = choice(render_dict['host'].split(','))
                logging.debug('chose %s for query execution', render_dict['host'])            
            if hide_password:
                hidden_pass_render_dict = render_dict.copy()
                hidden_pass_render_dict['password'] = 'XXXXXXX'
                result = bash_command.format(**hidden_pass_render_dict)
            else:
                result = bash_command.format(**render_dict)
        return result

    def run(self, bash_command, log_stdout=True, return_stdout=False, displayed_bash_command=None, exception_on_non_zero_code=True):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards

        :param bash_command: Bash command to be executed
        :param log_stdout: Log stdout to airflow log file (default: True)
        :param return_stdout: Return stdout text as return value of function (Default: False)
        :param displayed_bash_command: prints this commands to logs if passed
        :param exception_on_non_zero_code: Create AirflowException when command return non zero result code, enabled by default

        Example usage:
        ```
            from hooks.bash_hook import BashHook

            hook = BashHook()
            hook.run('ls -lh')
        ```
        """
        logging.debug('Tmp dir root location: \n %s', gettempdir())

        # Prepare env for child process.
        env = os.environ.copy()

        rendered_bash_command = self.render_command(bash_command)
        hidden_pass_bash_command = self.render_command(bash_command, hide_password=True)

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            logging.info('Running command: %s' % (displayed_bash_command or hidden_pass_bash_command))

            sub_process = Popen(  # pylint: disable=subprocess-popen-preexec-fn
                ['bash', "-c", rendered_bash_command],
                stdout=PIPE,
                stderr=STDOUT,
                cwd=tmp_dir,
                env=env,
                preexec_fn=pre_exec
            )

            result = ''
            for raw_line in iter(sub_process.stdout.readline, b''):
                line = raw_line.decode('utf-8')
                if log_stdout:
                    logging.info("%s", line.rstrip())
                    if 'DB::Exception' in line.rstrip():
                        raise AirflowException('DB::Exception')
                if return_stdout:
                    result += line

            sub_process.wait()

            logging.info('Command exited with return code %s', sub_process.returncode)

            if exception_on_non_zero_code and sub_process.returncode != 0:
                raise AirflowException(
                    f"Bash command failed. The command returned {sub_process.returncode} exit code.",
                    sub_process.returncode,
                )

        return result
