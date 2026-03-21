#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="apex-ledger"

mkdir -p "$PROJECT_ROOT"
cd "$PROJECT_ROOT"

mkdir -p \
  ledger/schema \
  ledger/domain/aggregates \
  ledger/tests

touch \
  README.md \
  pyproject.toml \
  requirements.txt \
  pytest.ini \
  .gitignore \
  ledger/__init__.py \
  ledger/schema.sql \
  ledger/event_store.py \
  ledger/schema/__init__.py \
  ledger/schema/events.py \
  ledger/domain/__init__.py \
  ledger/domain/aggregates/__init__.py \
  ledger/domain/aggregates/loan_application.py \
  ledger/domain/aggregates/agent_session.py \
  ledger/tests/__init__.py \
  ledger/tests/test_concurrency.py

cat > .gitignore <<'EOF'
__pycache__/
*.py[cod]
*.pyo
*.pyd
.Python
.env
.venv/
venv/
ENV/
env/
build/
dist/
*.egg-info/
.pytest_cache/
.mypy_cache/
.ruff_cache/
.coverage
coverage.xml
htmlcov/
.DS_Store
.idea/
.vscode/
EOF

chmod +x "$0" 2>/dev/null || true

printf 'Scaffold created in %s\n' "$(pwd)"