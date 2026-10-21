# CI notes (`cloud/ci`)

## What CI runs (`.github/workflows/ci.yml`)

- **lint** (Python 3.12): `ruff check`, `ruff format --check`, `mypy` (config in `pyproject.toml`, checks `src/`).
- **test** (Python 3.11, 3.12): `pytest -m "not live" tests/unit tests/integration` — unit tests and the
  in-process mock RTZR server tests only. No secrets are configured; real-API tests must be marked
  `@pytest.mark.live` (marker registered in `pyproject.toml`, `--strict-markers` on) and are skipped.
- Python 3.10 is not in the matrix: `pipecat-ai>=1.13` requires Python >= 3.11, so it cannot be installed.

## Local results (2026-10-21)

| Check | Result |
| --- | --- |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 15 files already formatted |
| `mypy` | Success: no issues found in 3 source files |
| `pytest -m "not live"` on 3.12 | 24 passed |
| `pytest -m "not live"` on 3.11 | 24 passed |
| `uv build` | wheel contains `pipecat_rtzr/{__init__,auth,stt}.py`, `py.typed` |

## Source changes (type fixes only, no behavior change)

- `stt.py`: annotate with `RTZRSTTSettings` instead of the `Settings` class alias (mypy rejects a class
  attribute as a type), type `_receive_task` as `asyncio.Task | None`, and `# type: ignore[override]`
  on `setup` / `_update_settings`, whose signatures mirror Pipecat's own `STTService` (the LSP
  mismatch is upstream's).
- Added `py.typed` marker.
