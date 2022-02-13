from contextlib import contextmanager
from enum import Enum
import json
import os
import signal
from subprocess import Popen
import sys
from typing import Any, Callable, Optional
import psutil
import logging
from functools import cached_property
import shutil

import dataset
import pandas as pd
import typer
import questionary
import tailer

on_windows = sys.platform == "win32"

logging.basicConfig(
    format="%(asctime)s - %(process)d - [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


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
        return dataset.connect(f"sqlite:///{self.file('execution.db')}")


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
        status_getter: Callable[[Execution], dict] = None,
    ) -> None:
        super().__init__(home, name)
        self.runner = runner
        self.default_config = default_config
        self.status_getter = status_getter

        if not os.path.isdir(self.home):
            os.makedirs(self.home, exist_ok=True)

    def execution_list(self) -> list[Execution]:
        return [
            Execution(os.path.join(self.home, d), d)
            for d in os.listdir(self.home)
            if os.path.isdir(os.path.join(self.home, d))
        ]

    def execution_map(self) -> dict[str, Execution]:
        return {e.name: e for e in self.execution_list()}

    @cached_property
    def cli(self) -> typer.Typer:
        app = typer.Typer()

        def select_execution(name: str = None, message: str = None) -> Execution:
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

        def execute(e: Execution) -> None:
            import atexit

            execution_pid = os.getpid()
            try:
                # TODO acquire a file lock before update the pid file
                e.set_pid(execution_pid)
            except ExecutionException:
                e.logger.exception(f"Execution process {execution_pid} aborted.")
                return
            except:
                self.logger.exception(
                    f"Faied to set execution pid {execution_pid} for '{e.name}'"
                )

            @atexit.register
            def bye():
                e.logger.info(f"Exit {execution_pid}")

            try:
                self.runner(e)
                e.logger.info("Execution finished")
            except Exception as exc:
                e.logger.exception(exc)

        def start_execution_daemon(e: Execution) -> None:
            if on_windows:
                # from win32process import DETACHED_PROCESS
                DETACHED_PROCESS = 8
                pid = Popen(
                    [sys.executable, sys.argv[0], "start", e.name],
                    creationflags=DETACHED_PROCESS,
                    shell=False,
                    close_fds=True,
                ).pid
                e.logger.info(f"running on windows: {pid}")
            else:
                import daemon

                with daemon.DaemonContext():
                    execute(e.clone())

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
        def config(name: Optional[str] = typer.Argument(None), reset: bool = False):
            if e := select_execution(name):
                if reset:
                    typer.echo(f"Reset config to default value ...")
                    e.write_config(self.default_config)
                typer.echo(f"Config: {e.config_file}")
                if on_windows:
                    os.system(f"notepad '{e.config_file}'")
                else:
                    os.system(f"vi '{e.config_file}'")

        @app.command(help=f"Stop execution")
        def stop(name: Optional[str] = typer.Argument(None)):
            if e := select_execution(name):
                e.stop()

        @app.command(help=f"Remove execution")
        def remove(name: Optional[str] = typer.Argument(None)):
            if e := select_execution(name):
                if e.status() == ExecutionStatus.running:
                    typer.echo(f"Execution '{e.name}' is running. Please stop it first")
                e.delete()

        @app.command(help=f"Check execution log")
        def logs(
            name: Optional[str] = typer.Argument(None),
            open_with: str = None,
            tail: bool = False,
        ):
            if e := select_execution(name):
                if open_with:
                    os.system(f"{open_with} '{e.log_file}'")
                else:
                    with open(e.log_file, "r") as fp:
                        print("\n".join(tailer.tail(fp, 100)))
                        if tail:
                            for line in tailer.follow(fp):
                                print(line)

        @app.command(help=f"New execution")
        def new(name: Optional[str] = typer.Argument(None)):
            if not name:
                name = questionary.text(
                    "Name", validate=lambda x: bool(x.strip())
                ).ask()

            e = Execution(os.path.join(self.home, name), name)
            if e.status() != ExecutionStatus.not_found:
                typer.echo(f"The name '{name}' has been used.", fg=typer.color.RED)
                raise typer.Exit(1)

            e.init()
            e.write_config(self.default_config or {})

            if on_windows:
                os.system(f"notepad '{e.config_file}'")
            else:
                os.system(f"vi '{e.config_file}'")

            if questionary.confirm("Start the exeution?", default=False).ask():
                start_execution_daemon(e)

        @app.command(help=f"Start execution")
        def start(name: Optional[str] = typer.Argument(None), service: bool = False):
            if e := select_execution(name):
                if service:
                    start_execution_daemon(e)
                else:
                    execute(e)

        return app
