#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${WHISPER_LORA_VENV:-/home/ned/venv_whisper52}"
PYTHON="$VENV_DIR/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  echo "LoRA Python not found: $PYTHON" >&2
  exit 1
fi

echo "Using LoRA environment: $VENV_DIR"
echo "Python: $PYTHON"
exec "$PYTHON" "$ROOT_DIR/train_whisper_lora.py" \
  --config "$ROOT_DIR/training_config_v106_lora_clean.json" "$@"
