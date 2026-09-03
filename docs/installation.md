# Installation and persistence

Use a project-local virtual environment so MWF and its dependencies remain
isolated from the system Python installation.

## Editable development install

From the repository directory containing `pyproject.toml`:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
```

On Linux or WSL, activate with `source .venv/bin/activate`. An editable install
uses the current source tree, so source changes are visible without rebuilding.

## Build release artifacts

Install the standard build frontend, then build both artifacts:

```powershell
python -m pip install --upgrade build
python -m build
```

The output under `dist/` should include:

```text
micro_workflow_manager-0.6.1-py3-none-any.whl
micro_workflow_manager-0.6.1.tar.gz
```

Build only the wheel with `python -m build --wheel`. The `py3-none-any` tag
means the wheel contains pure Python for Python 3 and no platform-specific
compiled extension.

Release verification is more than a successful build. Follow
[testing.md](testing.md): inspect artifact metadata and file lists, test the
extracted source archive in a fresh Test Area, and install the copied wheel into
a fresh environment before running import, CLI, and selected example checks.

## Install a wheel

Give pip the actual wheel path:

```powershell
python -m pip install --force-reinstall `
  .\dist\micro_workflow_manager-0.6.1-py3-none-any.whl
```

On Linux or WSL:

```bash
python -m pip install --force-reinstall \
  ./dist/micro_workflow_manager-0.6.1-py3-none-any.whl
```

A project may keep the wheel under `vendor/` and reference it from
`requirements.txt`:

```text
./vendor/micro_workflow_manager-0.6.1-py3-none-any.whl
```

Verify the installed version, import location, and CLI:

```powershell
python -c "import micro_workflow_manager as mwf; print(mwf.__version__); print(mwf.__file__)"
mwf --help
```

## Uninstall

Stop active `mwf run`, `runfrom`, `resume`, `resumefrom`, `monitor`, and `top`
processes before uninstalling. On Windows, an active launcher may be locked.

```powershell
python -m pip uninstall micro-workflow-manager
```

Deleting a project-local `.venv` removes that entire isolated Python
environment. MWF installs no Windows service, daemon, scheduled task, registry
entry, or persistent global worker.

## Project data persists

Uninstalling the package does not remove project data. Graph code, root and node
README files, `node/`, `.mwf/`, clipboard copies, deployment archives, and other
project files remain until the user deliberately removes them.

Treat `.mwf/state.sqlite3` as authoritative queue and runtime state. Do not
delete `.mwf/` or `node/` as part of a routine package upgrade.

## Repair an interrupted pip operation

If an older interrupted operation leaves an invalid distribution such as
`~icro-workflow-manager`, close Python and MWF processes and remove only the
matching temporary entries from that virtual environment. Then reinstall or
uninstall normally:

```powershell
Get-ChildItem .\.venv\Lib\site-packages -Force |
  Where-Object { $_.Name -like "~icro*" } |
  Remove-Item -Recurse -Force
Remove-Item .\.venv\Scripts\mwf.exe -Force -ErrorAction SilentlyContinue
python -m pip install --force-reinstall .
```

Do not use this cleanup against a system environment or a broad path.
