from inspect import Parameter, signature
from threading import RLock
from typing import Callable

from .models import MountedTask


def validate_non_negative_int(name: str, value: int) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be an integer >= 0")
    return value


def validate_positive_int(name: str, value: int) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be an integer >= 1")
    return value


class JobNode:
    def __init__(self, name: str, max_threads: int = 5):
        self.name = name
        self.max_threads = validate_positive_int("max_threads", max_threads)
        self.main_task: MountedTask | None = None
        self.fallbacks: dict[str, MountedTask] = {}
        self.fallback_order: list[str] = []
        self.lock = RLock()

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
    ):
        retries = validate_non_negative_int("retries", retries)
        repeats = validate_positive_int("repeats", repeats)

        allowed, required = self.infer_params(handler)

        self.main_task = MountedTask(
            name=handler.__name__,
            handler=handler,
            allowed_params=allowed,
            required_params=required,
            retries=retries,
            repeats=repeats,
        )

    def mount_fallback(
        self,
        handler: Callable,
        name: str | None = None,
        retries: int = 0,
        repeats: int = 1,
    ):
        retries = validate_non_negative_int("retries", retries)
        repeats = validate_positive_int("repeats", repeats)

        fallback_name = name or handler.__name__
        allowed, required = self.infer_params(handler)

        self.fallbacks[fallback_name] = MountedTask(
            name=fallback_name,
            handler=handler,
            allowed_params=allowed,
            required_params=required,
            retries=retries,
            repeats=repeats,
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
