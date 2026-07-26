#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config_v201_layered.json}"

python="${venv_dir}/bin/python"
if [[ ! -x "${python}" ]]; then
  echo "Whisper Python not found: ${python}" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to read the V201 config." >&2
  exit 1
fi

base_model="$(jq -er '.model' "${config_file}")"
manifests_dir="$(jq -er '.manifests_dir' "${config_file}")"
stage1_dir="$(jq -er '.staged_training.stage1.output_dir' "${config_file}")"
stage2_dir="$(jq -er '.staged_training.stage2.output_dir' "${config_file}")"

for output_path in "${stage1_dir}" "${stage2_dir}"; do
  if [[ -d "${output_path}" ]] && [[ -n "$(find "${output_path}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Refusing to overwrite non-empty output: ${output_path}" >&2
    exit 1
  fi
done

if [[ "$(jq -er '.train_rows' "${manifests_dir}/audit.json")" != "1082" ]]; then
  echo "V201 manifest audit does not contain exactly 1082 training calls." >&2
  exit 1
fi
if [[ "$(jq -er '.eval_rows' "${manifests_dir}/audit.json")" != "35" ]]; then
  echo "V201 manifest audit does not contain exactly 35 evaluation calls." >&2
  exit 1
fi
if [[ -e "${manifests_dir}/test.jsonl" ]]; then
  echo "Refusing V201 run because a test manifest exists." >&2
  exit 1
fi

run_stage() {
  local stage_key="$1"
  local model_path="$2"
  local output_path="$3"
  local stage_name epochs learning_rate augment_probability eval_steps save_steps acoustic_blocks

  stage_name="$(jq -er ".staged_training.${stage_key}.name" "${config_file}")"
  epochs="$(jq -er ".staged_training.${stage_key}.epochs" "${config_file}")"
  learning_rate="$(jq -er ".staged_training.${stage_key}.learning_rate" "${config_file}")"
  augment_probability="$(jq -er ".staged_training.${stage_key}.augment_probability" "${config_file}")"
  eval_steps="$(jq -er ".staged_training.${stage_key}.eval_steps" "${config_file}")"
  save_steps="$(jq -er ".staged_training.${stage_key}.save_steps" "${config_file}")"
  acoustic_blocks="$(jq -r ".staged_training.${stage_key}.acoustic_blocks // 24" "${config_file}")"

  echo "Starting V201 ${stage_key}: ${stage_name}"
  echo "Input model: ${model_path}"
  echo "Training calls: 1082; evaluation calls: 35; test calls: 0"

  "${python}" "${training_dir}/train_whisper.py" \
    --config "${config_file}" \
    --model "${model_path}" \
    --manifests-dir "${manifests_dir}" \
    --output-dir "${output_path}" \
    --training-stage "${stage_name}" \
    --acoustic-blocks "${acoustic_blocks}" \
    --epochs "${epochs}" \
    --learning-rate "${learning_rate}" \
    --augment-probability "${augment_probability}" \
    --eval-steps "${eval_steps}" \
    --save-steps "${save_steps}" \
    --metric-for-best-model selection_score \
    --no-run-test
}

echo "Using clean Whisper base: ${base_model}"
echo "Using locked V201 manifests: ${manifests_dir}"

run_stage stage1 "${base_model}" "${stage1_dir}"
run_stage stage2 "${stage1_dir}/best" "${stage2_dir}"

echo "V201 acoustic and language training complete."
echo "Acoustic checkpoint: ${stage1_dir}/best"
echo "Layered checkpoint: ${stage2_dir}/best"
