#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config_v110_lora.json}"

"${venv_dir}/bin/python" "${training_dir}/train_whisper_lora.py" --config "${config_file}"
"${venv_dir}/bin/python" "${training_dir}/convert_to_ctranslate2.py" \
  --config "${config_file}" \
  --quantization float16

echo "V110 reconstructed V101-style LoRA training and conversion complete."
