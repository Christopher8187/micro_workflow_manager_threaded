"""Attach recovery context without replacing the original exception."""


def add_recovery_note(error, note):
    error.__notes__ = [*getattr(error, '__notes__', ()), note]
