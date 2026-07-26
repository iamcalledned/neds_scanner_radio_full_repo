#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config_v112_corrective_lora.json}"
manifests_dir="$(jq -er '.manifests_dir' "${config_file}")"

if [[ "$(jq -er '.source_train_unique_ids' "${manifests_dir}/audit.json")" != "1082" ]]; then
  echo "V112 manifest does not retain all 1082 unique calls." >&2
  exit 1
fi
if [[ "$(jq -er '.eval_rows' "${manifests_dir}/audit.json")" != "35" ]]; then
  echo "V112 manifest does not contain exactly 35 eval calls." >&2
  exit 1
fi
if [[ -e "${manifests_dir}/test.jsonl" ]]; then
  echo "Refusing V112 run because a test manifest exists." >&2
  exit 1
fi

"${venv_dir}/bin/python" "${training_dir}/train_whisper_lora.py" --config "${config_file}"
"${venv_dir}/bin/python" "${training_dir}/convert_to_ctranslate2.py" \
  --config "${config_file}" \
  --quantization float16

echo "V112 corrective LoRA training and conversion complete."
