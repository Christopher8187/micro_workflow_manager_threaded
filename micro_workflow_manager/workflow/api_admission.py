"""Runtime ownership for one native project-wide API execution permit."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from time import sleep

from ..fibers import cooperative_sleep
from ..storage.recovery_errors import add_recovery_note
from .admission_wait import resolve_admission_future


class ApiPermitStateError(RuntimeError):
    """Durable API permit ownership could not be established or released."""


class ApiPermitAdmissionError(ApiPermitStateError):
    """A claimed API job could not establish its durable permit."""


class ApiPermitReleaseError(ApiPermitStateError):
    """An API handler stopped, but its durable permit did not release."""


@dataclass(slots=True)
class _PermitController:
    system: object
    session_id: str
    node_name: str
    job_id: int
    generation: int
    execution_id: str
    held: bool = False

    @property
    def identity(self):
        return (
            self.session_id, self.node_name, self.job_id,
            self.generation, self.execution_id,
        )

    def _check(self):
        self.system.check_job_execution(
            self.node_name, self.job_id, self.generation, self.execution_id,
        )

    def _pause_children(self):
        if not self.system.storage._has_interrupt_pause_candidate(
            self.execution_id,
        ):
            return ()
        return self.system.storage.read_active_interrupt_pauses(*self.identity)

    def _sleep_before_retry(self):
        if not cooperative_sleep(0.05):
            sleep(0.05)

    def _wait_for_prepermit_pause(self) -> bool:
        """Acknowledge an interrupt while this execution owns no capacity."""
        if self.held:
            raise RuntimeError("Pre-permit pause wait still owns API capacity")
        try:
            children = self._pause_children()
            if not children:
                return False
            children = self.system.storage.acknowledge_interrupt_pauses(
                *self.identity,
            )
            while children:
                self._check()
                self._sleep_before_retry()
                self._check()
                children = self._pause_children()
            return True
        except ApiPermitStateError:
            raise
        except BaseException as error:
            # Prefer the established restart signal if the pause state changed
            # because this claimed execution was superseded.
            self._check()
            raise ApiPermitAdmissionError(
                "Claimed API execution could not resolve its interrupt pause"
            ) from error

    def acquire(self):
        if self.held:
            return
        while True:
            self._check()
            if self._wait_for_prepermit_pause():
                continue
            try:
                pending = self.system.storage.try_acquire_api_execution_permit(
                    *self.identity, _wait=False,
                )
                try:
                    settled = resolve_admission_future(
                        pending,
                        lambda: self.system.storage._read_api_execution_permit_decision(
                            *self.identity, present=True,
                        ),
                    )
                except BaseException as error:
                    # Resolve capacity ownership before considering a restart.
                    # A writer may have committed even when delivery/readback
                    # first failed, and advancing the job must not hide that
                    # retained permit.
                    try:
                        durable = (
                            self.system.storage._read_api_execution_permit_decision(
                                *self.identity, present=True,
                            )
                        )
                    except BaseException as read_error:
                        failure = ApiPermitAdmissionError(
                            "API permit admission outcome could not be resolved"
                        )
                        add_recovery_note(
                            failure,
                            f"Admission writer also stopped with: {error}",
                        )
                        raise failure from read_error
                    if durable is True:
                        self.held = True
                        try:
                            self.release()
                        except ApiPermitStateError as release_error:
                            add_recovery_note(
                                release_error,
                                f"Permit admission also stopped with: {error}",
                            )
                            raise
                        raise ApiPermitAdmissionError(
                            "API permit admission stopped after its durable insert"
                        ) from error
                    if durable is not None:
                        raise ApiPermitAdmissionError(
                            "API permit admission readback returned an invalid result"
                        ) from error
                    self._check()
                    raise ApiPermitAdmissionError(
                        "Claimed API execution could not establish its durable permit"
                    ) from error
                acquired = settled.value
                if type(acquired) is not bool:
                    raise RuntimeError("API permit admission returned a non-Boolean result")
                if acquired:
                    self.held = True
                if settled.interruption is not None:
                    failure = ApiPermitAdmissionError(
                        "API permit admission was interrupted after its writer decision"
                    )
                    add_recovery_note(
                        failure,
                        f"Admission wait also stopped with: {settled.interruption}",
                    )
                    if self.held:
                        try:
                            self.release()
                        except BaseException as release_error:
                            add_recovery_note(
                                release_error,
                                f"Permit admission was interrupted by: {settled.interruption}",
                            )
                            raise
                    raise failure from settled.interruption
            except ApiPermitStateError:
                # Capacity may still be owned. Preserve this failure so a
                # concurrent restart cannot mask it and claim a replacement.
                raise
            except BaseException as error:
                # Prefer the established restart signal if the writer refused
                # because this generation changed during admission, but only
                # after the durable readback above established no permit.
                self._check()
                raise ApiPermitAdmissionError(
                    "Claimed API execution could not establish its durable permit"
                ) from error
            if acquired:
                try:
                    self._check()
                except BaseException as error:
                    try:
                        self.release()
                    except ApiPermitStateError as release_error:
                        add_recovery_note(
                            release_error,
                            f"Permit currency check also stopped with: {error}",
                        )
                        raise release_error from error
                    raise
                try:
                    pause_pending = bool(self._pause_children())
                except BaseException as error:
                    try:
                        self.release()
                    except ApiPermitStateError as release_error:
                        add_recovery_note(
                            release_error,
                            f"Permit validation also stopped with: {error}",
                        )
                        raise release_error from error
                    raise ApiPermitAdmissionError(
                        "Claimed API execution could not validate interrupt pause state"
                    ) from error
                if pause_pending:
                    # The pause arrived during the atomic capacity decision.
                    # Yield this newly acquired slot before acknowledgement.
                    self.release()
                    self._wait_for_prepermit_pause()
                    continue
                return
            self._check()
            self._sleep_before_retry()

    def release(self):
        if not self.held:
            return
        try:
            pending = self.system.storage.release_api_execution_permit(
                *self.identity, _wait=False,
            )
            settled = resolve_admission_future(
                pending,
                lambda: self.system.storage._read_api_execution_permit_decision(
                    *self.identity, present=False,
                ),
            )
            if settled.value is not True:
                raise RuntimeError("API permit release returned an invalid result")
            self.held = False
            if settled.interruption is not None:
                raise ApiPermitReleaseError(
                    "API permit release was interrupted after its writer decision"
                ) from settled.interruption
        except BaseException as error:
            raise ApiPermitReleaseError(
                "API execution permit could not be released"
            ) from error


_CURRENT_API_PERMIT: ContextVar[_PermitController | None] = ContextVar(
    "mwf_current_api_permit", default=None,
)


def _context_controller(context):
    controller = _CURRENT_API_PERMIT.get()
    if controller is None:
        return None
    session_id, _ = context.system.execution_claim_context(context.current_node)
    expected = (
        session_id, context.current_node, context.job_id,
        context.execution_generation, context.execution_id,
    )
    if controller.system is not context.system or controller.identity != expected:
        raise RuntimeError("Checkpoint API permit does not match its execution")
    return controller


def release_current_api_execution_permit(context) -> bool:
    """Yield the current API permit at a safe cooperative checkpoint."""
    controller = _context_controller(context)
    if controller is None:
        return False
    controller.release()
    return True


def reacquire_current_api_execution_permit(context) -> bool:
    """Reacquire the yielded permit before returning to the API handler."""
    controller = _context_controller(context)
    if controller is None:
        return False
    controller.acquire()
    return True


@contextmanager
def api_execution_permit(system, node_name, job_id, generation, execution_id):
    node = system.nodes[node_name]
    if (node.runner_override or system.runner) != "api":
        yield None
        return
    session_id, _ = system.execution_claim_context(node_name)
    controller = _PermitController(
        system=system,
        session_id=session_id,
        node_name=node_name,
        job_id=job_id,
        generation=generation,
        execution_id=execution_id,
    )
    controller.acquire()
    token = _CURRENT_API_PERMIT.set(controller)
    primary_error = None
    try:
        yield controller
    except BaseException as error:
        primary_error = error
        raise
    finally:
        try:
            controller.release()
        except BaseException as release_error:
            failure = ApiPermitReleaseError(
                "API execution stopped before its durable permit could be released"
            )
            if primary_error is not None:
                add_recovery_note(
                    failure,
                    f"The API handler also stopped with: {primary_error}",
                )
            raise failure from release_error
        finally:
            _CURRENT_API_PERMIT.reset(token)
