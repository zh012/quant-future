import queue
import socket
from contextlib import closing
from enum import Enum
import json
import os
import signal
from subprocess import Popen
import sys
import threading
from typing import Any, Callable, Optional
import psutil
import logging
from functools import cached_property
import shutil

import dataset
import pandas as pd
import typer
import questionary
import requests
import notifypy


on_windows = sys.platform == "win32"
script_dir = os.path.abspath(os.path.split(__file__)[0])

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] (%(name)s %(process)d) %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def edit_file(fn, open_with=None):
    if on_windows:
        os.system(f"{open_with or 'notepad'} '{fn}'")
    else:
        os.system(f"{open_with or 'code'} '{fn}'")


class Notifier:
    def __init__(
        self,
        *,
        logger: logging.Logger = None,
        telegram: dict = None,
        desktop: bool = False,
        title: str = "Notice",
    ) -> None:
        self.queue = queue.Queue()
        self.telegram = telegram
        self.desktop = desktop
        self.logger = logger
        self.title = title

        if telegram:
            self.telegram_channel_url = f"https://api.telegram.org/bot{telegram['bot']}/sendMessage?chat_id=-100{telegram['channel']}&text={{}}"
        else:
            self.telegram_channel_url = None

    def run(self) -> None:
        while msg := self.queue.get():
            if self.desktop:
                try:
                    n = notifypy.Notify(
                        default_notification_icon=os.path.join(
                            script_dir, "gold-bar.png"
                        )
                    )
                    n.title = msg[0]
                    n.message = msg[1]
                    n.send()
                except:
                    if self.logger:
                        self.logger.error(f"failed to send desktop notfication {msg}")
            if self.telegram_channel_url:
                try:
                    requests.get(
                        self.telegram_channel_url.format(f"{msg[0]}\n{msg[1]}"),
                        timeout=1,
                    )
                except:
                    if self.logger:
                        self.logger.error(f"failed to send desktop notfication {msg}")
            self.logger.info(f"Notify: {msg}")

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run)
        self.thread.setDaemon(True)
        self.thread.start()

    def send(self, message: str, title: str = None) -> None:
        self.queue.put((title or self.title, message))


class ExecutionException(Exception):
    pass


class ExecutionStatus(str, Enum):
    not_found = "not_found"
    stopped = "stopped"
    running = "running"
    abnormal_pid = "abnormal_pid"
    abnormal_proc = "abnormal_proc"

    def tr_en(self):
        return {
            "not_found": "Not exists",
            "stopped": "Stopped",
            "running": "Running",
            "abnormal_pid": "Abnormal",
            "abnormal_proc": "Abnoram",
        }[self.value]

    def tr_zh(self):
        return {
            "not_found": "不存在",
            "stopped": "未运行",
            "running": "运行中",
            "abnormal_pid": "异常(pid)",
            "abnormal_proc": "异常(proc)",
        }[self.value]


CONFIG_FILE_NAME = "config.json"


def normabspath(path: str) -> str:
    return os.path.normpath(os.path.abspath(os.path.expanduser(path)))


class Workspace:
    def __init__(self, home: str, name: str = None) -> None:
        self.home = normabspath(home)
        self.name = self.name = (
            name or os.path.splitext(os.path.split(home.rstrip("/"))[1])[0]
        )

    def delete(self):
        shutil.rmtree(self.home, ignore_errors=True)

    def file(self, name: str) -> str:
        return os.path.join(self.home, name)

    def write_text(self, file_name: str, content: str) -> None:
        with open(self.file(file_name), "w") as fp:
            fp.write(content)

    def read_text(self, file_name: str) -> str:
        with open(self.file(file_name), "r") as fp:
            return fp.read()

    def write_json(self, file_name: str, data: Any) -> None:
        self.write_text(
            file_name, json.dumps(data, indent=4, default=str, ensure_ascii=False)
        )

    def read_json(self, file_name: str, safe: bool = False) -> Any:
        try:
            return json.loads(self.read_text(file_name))
        except:
            raise
            if not safe:
                raise

    def write_csv(self, file_name: str, df: pd.DataFrame, **pdargs: Any) -> None:
        df.to_csv(self.file(file_name), **pdargs)

    def read_csv(
        self, file_name: str, safe: bool = False, **pdargs: Any
    ) -> pd.DataFrame:
        try:
            return pd.read_csv(self.file(file_name), **pdargs)
        except:
            if not safe:
                raise

    @cached_property
    def log_file(self) -> str:
        return self.file("log.txt")

    @cached_property
    def logger(self) -> logging.Logger:
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(process)d - [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(fh)

        return logger

    @cached_property
    def db(self) -> dataset.Database:
        return dataset.connect(f"sqlite:///{self.file('__db__')}")


class Execution(Workspace):
    @cached_property
    def pid_file(self) -> str:
        return self.file("__pid__")

    @cached_property
    def config_file(self) -> str:
        return self.file(CONFIG_FILE_NAME)

    def init(self) -> None:
        if not os.path.isdir(self.home):
            os.makedirs(self.home, exist_ok=True)
        if not os.path.isfile(self.log_file):
            open(self.log_file, "w").close()
        if not os.path.isfile(self.config_file):
            self.write_config({})

    def status(self) -> ExecutionStatus:
        if not os.path.isdir(self.home):
            return ExecutionStatus.not_found

        if not os.path.isfile(self.pid_file):
            return ExecutionStatus.stopped

        if not (pid := self.get_pid()):
            return ExecutionStatus.abnormal_pid
        elif proc := next((p for p in psutil.process_iter() if p.pid == pid), None):
            return (
                proc.is_running()
                and ExecutionStatus.running
                or ExecutionStatus.abnormal_proc
            )

        return ExecutionStatus.stopped

    def get_pid(self) -> int:
        try:
            return int(self.read_text(self.pid_file).strip())
        except:
            return None

    def set_pid(self, pid: int) -> None:
        if self.status() == ExecutionStatus.running:
            raise ExecutionException("The execution is already running")
        with open(self.pid_file, "w") as fp:
            fp.write(str(pid))

    def read_config(self) -> dict:
        return self.read_json(CONFIG_FILE_NAME, safe=True)

    def write_config(self, data: dict) -> None:
        return self.write_json(CONFIG_FILE_NAME, data)

    def stop(self, force: bool = False):
        if pid := self.get_pid():
            os.kill(pid, force and signal.SIGKILL or signal.SIGINT)

    def clone(self) -> "Execution":
        return Execution(self.home, self.name)


class App(Workspace):
    def __init__(
        self,
        *,
        home: str,
        runner: Callable[[Execution], None],
        name: str = None,
        default_config: dict = None,
    ) -> None:
        super().__init__(home, name)
        self.runner = runner
        self.default_config = default_config

        if not os.path.isdir(self.home):
            os.makedirs(self.home, exist_ok=True)

    def read_config(self) -> dict:
        return self.read_json(CONFIG_FILE_NAME, safe=True)

    def execution_list(self) -> list[Execution]:
        return [
            Execution(os.path.join(self.home, d), d)
            for d in os.listdir(self.home)
            if os.path.isdir(os.path.join(self.home, d))
        ]

    def execution_map(self) -> dict[str, Execution]:
        return {e.name: e for e in self.execution_list()}

    def select_execution(self, name: str = None, message: str = None) -> Execution:
        es = self.execution_map()

        if not es:
            typer.echo(f"No execution is found")
            return

        if e := es.get(name):
            return e

        message = message or "Please select one execution:"
        if name:
            message = f"The execution '{name}' does not exist.\n{message}"

        name = questionary.select(message, es.keys()).ask()
        if name:
            return es[name]

    def execute(self, e: Execution) -> None:
        import atexit

        execution_pid = os.getpid()
        try:
            # TODO acquire a file lock before update the pid file
            e.set_pid(execution_pid)
        except ExecutionException:
            e.logger.exception(f"Execution process {execution_pid} aborted.")
            return
        except:
            e.logger.exception(
                f"Faied to set execution pid {execution_pid} for '{e.name}'"
            )

        @atexit.register
        def bye():
            e.logger.info(f"Exit {execution_pid}")

        try:
            self.runner(e)
            e.logger.info(f"Finished {execution_pid}")
        except Exception as exc:
            e.logger.exception(exc)

    @cached_property
    def cli(self) -> typer.Typer:
        app = typer.Typer()

        def start_execution_daemon(e: Execution) -> None:
            output = open(e.file("output.txt"), "a")
            if on_windows:
                # from win32process import DETACHED_PROCESS
                DETACHED_PROCESS = 8
                Popen(
                    [sys.executable, sys.argv[0], "start", e.name],
                    creationflags=DETACHED_PROCESS,
                    close_fds=True,
                    stdout=output,
                    stderr=output,
                )
            else:

                def preexec_function():
                    signal.signal(signal.SIGHUP, signal.SIG_IGN)

                Popen(
                    [sys.executable, sys.argv[0], "start", e.name],
                    preexec_fn=preexec_function,
                    close_fds=True,
                    stdout=output,
                    stderr=output,
                )

        @app.callback(invoke_without_command=True, no_args_is_help=True)
        def help():
            pass

        @app.command(name="list", help="List the executions")
        def status():
            if es := self.execution_list():
                for e in es:
                    if e.status() == ExecutionStatus.running:
                        typer.secho(
                            f"{e.status().tr_en():<12} {e.name}",
                            fg=typer.colors.BLACK,
                            bg=typer.colors.WHITE,
                        )
                    else:
                        typer.echo(f"{e.status().tr_en():<12} {e.name}")
            else:
                typer.echo(
                    f"No execution found for application{self.name and ' ' + self.name or ''} at '{self.home}'"
                )
                raise typer.Exit(1)

        @app.command(help=f"Configure execution")
        def config(
            name: Optional[str] = typer.Argument(None),
            reset: bool = False,
            open_with: str = None,
        ):
            if e := self.select_execution(name):
                if reset:
                    typer.echo(f"Reset config to default value ...")
                    e.write_config(self.default_config)
                typer.echo(f"Config: {e.config_file}")
                edit_file(e.config_file, open_with)

        @app.command(help=f"Stop execution")
        def stop(name: Optional[str] = typer.Argument(None), force: bool = False):
            if e := self.select_execution(name):
                if e.status() in (
                    ExecutionStatus.running,
                    ExecutionStatus.abnormal_proc,
                ):
                    e.stop()

        @app.command(help=f"Remove execution")
        def remove(name: Optional[str] = typer.Argument(None)):
            if e := self.select_execution(name):
                if e.status() == ExecutionStatus.running:
                    typer.echo(f"Execution '{e.name}' is running. Please stop it first")
                e.delete()

        @app.command(help=f"Check execution log")
        def logs(
            name: Optional[str] = typer.Argument(None),
            file: Optional[str] = typer.Option(None, "-f", help="File name"),
            _print: bool = typer.Option(False, "-p", help="Print to console"),
            open_with: str = None,
        ):
            if e := self.select_execution(name):
                f = file and e.file(file) or e.log_file
                if _print:
                    with open(f, "r") as fp:
                        print(fp.read())
                else:
                    edit_file(f, open_with)

        @app.command(help=f"New execution")
        def new(
            name: Optional[str] = typer.Argument(None),
            clone: bool = False,
            open_with: str = None,
        ):
            if not name:
                name = questionary.text(
                    "Name", validate=lambda x: bool(x.strip())
                ).ask()

            e = Execution(os.path.join(self.home, name), name)
            if e.status() != ExecutionStatus.not_found:
                typer.echo(f"The name '{name}' has been used.", fg=typer.color.RED)
                raise typer.Exit(1)

            orig = None
            if clone:
                if not (orig := self.select_execution()):
                    raise typer.Exit(1)

            e.init()

            if orig:
                init_config = orig.read_config()
            elif self.default_config:
                init_config = self.default_config
            else:
                init_config = {}
            e.write_config(init_config)
            edit_file(e.config_file, open_with)

        @app.command(help=f"Start execution")
        def start(name: Optional[str] = typer.Argument(None), service: bool = False):
            if e := self.select_execution(name):
                if service:
                    start_execution_daemon(e)
                else:
                    self.execute(e)

        return app
