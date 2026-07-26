#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${training_dir}/training_config_v113_stage1_targeted.json"
audit_file="${training_dir}/manifests_v113_two_stage/audit.json"

[[ "$(jq -er '.source_train_unique_ids' "${audit_file}")" == "1082" ]]
[[ "$(jq -er '.eval_rows' "${audit_file}")" == "35" ]]
[[ "$(jq -er '.test_rows_loaded' "${audit_file}")" == "0" ]]
[[ ! -e "${training_dir}/manifests_v113_two_stage/stage1_targeted/test.jsonl" ]]

"${venv_dir}/bin/python" "${training_dir}/train_whisper_lora.py" --config "${config_file}"
"${venv_dir}/bin/python" "${training_dir}/convert_to_ctranslate2.py" \
  --config "${config_file}" --quantization float16
