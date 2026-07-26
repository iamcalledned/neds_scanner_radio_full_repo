#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config_v103_layered.json}"

if (($#)); then
  if [[ "$1" == "--config" && $# -eq 2 ]]; then
    config_file="$2"
  else
    echo "Usage: $0 [--config PATH]" >&2
    exit 2
  fi
fi

if [[ ! -f "${venv_dir}/bin/activate" || ! -x "${venv_dir}/bin/python" ]]; then
  echo "Whisper virtual environment not found: ${venv_dir}" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to read the staged training config." >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${venv_dir}/bin/activate"

base_model="$(jq -er '.model' "${config_file}")"
stage1_dir="$(jq -er '.staged_training.stage1.output_dir' "${config_file}")"
stage2_dir="$(jq -er '.staged_training.stage2.output_dir' "${config_file}")"
stage3_dir="$(jq -er '.staged_training.stage3.output_dir' "${config_file}")"

run_stage() {
  local stage_key="$1"
  local model_path="$2"
  local output_path="$3"
  local run_test="$4"
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
  echo "Output: ${output_path}"

  python "${training_dir}/train_whisper.py" \
    --config "${config_file}" \
    --model "${model_path}" \
    --output-dir "${output_path}" \
    --training-stage "${stage_name}" \
    --acoustic-blocks "${acoustic_blocks}" \
    --epochs "${epochs}" \
    --learning-rate "${learning_rate}" \
    --augment-probability "${augment_probability}" \
    --eval-steps "${eval_steps}" \
    --save-steps "${save_steps}" \
    "${run_test}"
}

echo "Using Whisper environment: ${VIRTUAL_ENV}"
echo "Python: $(command -v python)"
echo "Layered config: ${config_file}"

python "${training_dir}/prepare_data.py" --config "${config_file}"
python "${training_dir}/align_long_audio.py" --config "${config_file}"

run_stage stage1 "${base_model}" "${stage1_dir}" --no-run-test
run_stage stage2 "${stage1_dir}/best" "${stage2_dir}" --no-run-test
run_stage stage3 "${stage2_dir}/best" "${stage3_dir}" --run-test

echo
echo "Layered V103 training complete."
echo "Final Hugging Face model: ${stage3_dir}/best"
