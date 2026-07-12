from inspect import Parameter, signature
from threading import RLock
from typing import Callable

from .models import MountedTask


NODE_RUNNER_CHOICES = {"direct", "threaded", "process"}


def validate_non_negative_int(name: str, value: int) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be an integer >= 0")
    return value


def validate_positive_int(name: str, value: int) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be an integer >= 1")
    return value



def validate_positive_float(name: str, value: float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{name} must be a positive number or None")
    return float(value)


def validate_node_runner(runner: str | None) -> str | None:
    if runner is None:
        return None

    aliases = {
        "thread": "threaded",
        "processes": "process",
        "process_pool": "process",
        "processpool": "process",
    }
    runner = aliases.get(runner, runner)

    if runner not in NODE_RUNNER_CHOICES:
        raise ValueError(f"runner must be one of {sorted(NODE_RUNNER_CHOICES)}")

    return runner


def sequential_runner_value(
    runner: str | None = None,
    sequential: bool = False,
) -> str | None:
    runner = validate_node_runner(runner)

    if sequential:
        if runner not in {None, "direct"}:
            raise ValueError("sequential=True cannot be combined with a concurrent runner")
        return "direct"

    return runner


class JobNode:
    def __init__(
        self,
        name: str,
        max_threads: int = 5,
        runner: str | None = None,
    ):
        self.name = name
        self.max_threads = validate_positive_int("max_threads", max_threads)
        self.runner_override = validate_node_runner(runner)
        self.main_task: MountedTask | None = None
        self.fallbacks: dict[str, MountedTask] = {}
        self.fallback_order: list[str] = []
        self.lock = RLock()

    @property
    def sequential(self) -> bool:
        return self.runner_override == "direct"

    def set_runner(self, runner: str | None = None, sequential: bool = False):
        override = sequential_runner_value(runner=runner, sequential=sequential)

        if override is not None:
            self.runner_override = override

        if self.runner_override == "direct":
            self.max_threads = 1

    def infer_params(self, handler: Callable) -> tuple[set[str], set[str]]:
        sig = signature(handler)
        params = list(sig.parameters.values())

        if not params or params[0].name != "ctx":
            raise ValueError(
                f"{handler.__name__} must take ctx as its first parameter"
            )

        if params[0].kind in {Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD}:
            raise ValueError(f"{handler.__name__} cannot use *args or **kwargs")

        allowed: set[str] = set()
        required: set[str] = set()

        for param in params[1:]:
            name = param.name

            if param.kind in {Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD}:
                raise ValueError(f"{handler.__name__} cannot use *args or **kwargs")

            if name == "ctx":
                raise ValueError(
                    f"{handler.__name__} can only use ctx as the first parameter"
                )

            allowed.add(name)

            if param.default is Parameter.empty:
                required.add(name)

        return allowed, required

    def mount_main(
        self,
        handler: Callable,
        retries: int = 0,
        repeats: int = 1,
        timeout: float | None = None,
        checkpoint_timeout: float | None = None,
    ):
        retries = validate_non_negative_int("retries", retries)
        repeats = validate_positive_int("repeats", repeats)
        timeout = validate_positive_float("timeout", timeout)
        checkpoint_timeout = validate_positive_float("checkpoint_timeout", checkpoint_timeout)

        allowed, required = self.infer_params(handler)

        self.main_task = MountedTask(
            name=handler.__name__,
            handler=handler,
            allowed_params=allowed,
            required_params=required,
            retries=retries,
            repeats=repeats,
            timeout=timeout,
            checkpoint_timeout=checkpoint_timeout,
        )

    def mount_fallback(
        self,
        handler: Callable,
        name: str | None = None,
        retries: int = 0,
        repeats: int = 1,
        timeout: float | None = None,
        checkpoint_timeout: float | None = None,
    ):
        retries = validate_non_negative_int("retries", retries)
        repeats = validate_positive_int("repeats", repeats)
        timeout = validate_positive_float("timeout", timeout)
        checkpoint_timeout = validate_positive_float("checkpoint_timeout", checkpoint_timeout)

        fallback_name = name or handler.__name__
        allowed, required = self.infer_params(handler)

        self.fallbacks[fallback_name] = MountedTask(
            name=fallback_name,
            handler=handler,
            allowed_params=allowed,
            required_params=required,
            retries=retries,
            repeats=repeats,
            timeout=timeout,
            checkpoint_timeout=checkpoint_timeout,
        )

        if fallback_name not in self.fallback_order:
            self.fallback_order.append(fallback_name)

    def validate_params(self, params: dict):
        if self.main_task is None:
            raise ValueError(f"Node {self.name} has no mounted task")

        invalid = set(params) - self.main_task.allowed_params
        if invalid:
            raise ValueError(f"Invalid params for node {self.name}: {invalid}")

        missing = self.main_task.required_params - set(params)
        if missing:
            raise ValueError(f"Missing params for node {self.name}: {missing}")
