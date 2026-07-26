#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config_v107_layered.json}"

if [[ ! -x "${venv_dir}/bin/python" ]]; then
  echo "Whisper Python not found: ${venv_dir}/bin/python" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to read the layered training config." >&2
  exit 1
fi

python="${venv_dir}/bin/python"
base_model="$(jq -er '.model' "${config_file}")"
manifests_dir="$(jq -er '.manifests_dir' "${config_file}")"
negative_manifests_dir="$(jq -er '.negative_manifests_dir' "${config_file}")"
stage1_dir="$(jq -er '.staged_training.stage1.output_dir' "${config_file}")"
stage2_dir="$(jq -er '.staged_training.stage2.output_dir' "${config_file}")"
stage3_dir="$(jq -er '.staged_training.stage3.output_dir' "${config_file}")"

for output_path in "${stage1_dir}" "${stage2_dir}" "${stage3_dir}"; do
  if [[ -d "${output_path}" ]] && [[ -n "$(find "${output_path}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Refusing to overwrite non-empty output: ${output_path}" >&2
    exit 1
  fi
done

run_stage() {
  local stage_key="$1"
  local model_path="$2"
  local output_path="$3"
  local stage_manifests="$4"
  local run_test="$5"
  local stage_name epochs learning_rate augment_probability eval_steps save_steps acoustic_blocks

  stage_name="$(jq -er ".staged_training.${stage_key}.name" "${config_file}")"
  epochs="$(jq -er ".staged_training.${stage_key}.epochs" "${config_file}")"
  learning_rate="$(jq -er ".staged_training.${stage_key}.learning_rate" "${config_file}")"
  augment_probability="$(jq -er ".staged_training.${stage_key}.augment_probability" "${config_file}")"
  eval_steps="$(jq -er ".staged_training.${stage_key}.eval_steps" "${config_file}")"
  save_steps="$(jq -er ".staged_training.${stage_key}.save_steps" "${config_file}")"
  acoustic_blocks="$(jq -r ".staged_training.${stage_key}.acoustic_blocks // 3" "${config_file}")"

  echo
  echo "Starting ${stage_key}: ${stage_name}"
  echo "Input model: ${model_path}"
  echo "Manifests: ${stage_manifests}"
  echo "Output: ${output_path}"

  "${python}" "${training_dir}/train_whisper.py" \
    --config "${config_file}" \
    --model "${model_path}" \
    --manifests-dir "${stage_manifests}" \
    --output-dir "${output_path}" \
    --training-stage "${stage_name}" \
    --acoustic-blocks "${acoustic_blocks}" \
    --epochs "${epochs}" \
    --learning-rate "${learning_rate}" \
    --augment-probability "${augment_probability}" \
    --eval-steps "${eval_steps}" \
    --save-steps "${save_steps}" \
    --metric-for-best-model selection_score \
    "${run_test}"
}

echo "Using Whisper environment: ${venv_dir}"
echo "Layered config: ${config_file}"

if [[ -f "${negative_manifests_dir}/summary.json" ]]; then
  echo "Using existing negative-refinement manifests: ${negative_manifests_dir}"
else
  "${python}" "${training_dir}/build_v107_negative_manifests.py" \
    --source-dir "${manifests_dir}" \
    --output-dir "${negative_manifests_dir}" \
    --negative-repeat "$(jq -er '.staged_training.stage3.negative_repeat' "${config_file}")"
fi

run_stage stage1 "${base_model}" "${stage1_dir}" "${manifests_dir}" --no-run-test
run_stage stage2 "${stage1_dir}/best" "${stage2_dir}" "${manifests_dir}" --no-run-test
run_stage stage3 "${stage2_dir}/best" "${stage3_dir}" "${negative_manifests_dir}" --run-test

echo
echo "Layered V107 training complete."
echo "Final Hugging Face model: ${stage3_dir}/best"
