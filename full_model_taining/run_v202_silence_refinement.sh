#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config_v202_silence_refinement.json}"
python="${venv_dir}/bin/python"

if [[ ! -x "${python}" ]]; then
  echo "Whisper Python not found: ${python}" >&2
  exit 1
fi

model="$(jq -er '.model' "${config_file}")"
manifests_dir="$(jq -er '.manifests_dir' "${config_file}")"
output_dir="$(jq -er '.output_dir' "${config_file}")"

if [[ -d "${output_dir}" ]] && [[ -n "$(find "${output_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "Refusing to overwrite non-empty output: ${output_dir}" >&2
  exit 1
fi
if [[ "$(jq -er '.source_train_unique_ids' "${manifests_dir}/audit.json")" != "1082" ]]; then
  echo "V202 manifest does not retain all 1082 unique training calls." >&2
  exit 1
fi
if [[ "$(jq -er '.total_train_presentations' "${manifests_dir}/audit.json")" != "1257" ]]; then
  echo "V202 manifest does not contain exactly 1257 training presentations." >&2
  exit 1
fi
if [[ "$(jq -er '.eval_rows' "${manifests_dir}/audit.json")" != "35" ]]; then
  echo "V202 manifest does not contain exactly 35 evaluation calls." >&2
  exit 1
fi
if [[ -e "${manifests_dir}/test.jsonl" ]]; then
  echo "Refusing V202 run because a test manifest exists." >&2
  exit 1
fi

echo "Starting V202 conservative silence refinement"
echo "Base model: ${model}"
echo "Unique training calls: 1082"
echo "Non-speech presentations: 200"
echo "Evaluation calls: 35; test calls: 0"

"${python}" "${training_dir}/train_whisper.py" \
  --config "${config_file}" \
  --model "${model}" \
  --manifests-dir "${manifests_dir}" \
  --output-dir "${output_dir}" \
  --training-stage language \
  --epochs 1.0 \
  --learning-rate 0.0000003 \
  --augment-probability 0.0 \
  --eval-steps 26 \
  --save-steps 26 \
  --metric-for-best-model selection_score \
  --no-run-test

"${python}" "${training_dir}/convert_to_ctranslate2.py" \
  --config "${config_file}" \
  --quantization float16

echo "V202 training and CT2 conversion complete."
