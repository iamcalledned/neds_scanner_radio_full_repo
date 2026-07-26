#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config_v104.json}"

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
  echo "jq is required to read the training config." >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${venv_dir}/bin/activate"

base_model="$(jq -er '.model' "${config_file}")"
language_output="$(jq -er '.language_stage.output_dir' "${config_file}")"
consolidation_output="$(jq -er '.consolidation_stage.output_dir' "${config_file}")"

echo "Using Whisper environment: ${VIRTUAL_ENV}"
echo "Starting V104 from: ${base_model}"
echo "Data: $(jq -er '.manifests_dir' "${config_file}")"

python "${training_dir}/train_whisper.py" \
  --config "${config_file}" \
  --model "${base_model}" \
  --output-dir "${language_output}" \
  --training-stage language \
  --epochs "$(jq -er '.language_stage.epochs' "${config_file}")" \
  --learning-rate "$(jq -er '.language_stage.learning_rate' "${config_file}")" \
  --augment-probability "$(jq -er '.language_stage.augment_probability' "${config_file}")" \
  --eval-steps "$(jq -er '.language_stage.eval_steps' "${config_file}")" \
  --save-steps "$(jq -er '.language_stage.save_steps' "${config_file}")" \
  --metric-for-best-model selection_score \
  --run-test

echo
echo "Starting low-rate full-model consolidation from the best language checkpoint."

python "${training_dir}/train_whisper.py" \
  --config "${config_file}" \
  --model "${language_output}/best" \
  --output-dir "${consolidation_output}" \
  --training-stage consolidation \
  --epochs "$(jq -er '.consolidation_stage.epochs' "${config_file}")" \
  --learning-rate "$(jq -er '.consolidation_stage.learning_rate' "${config_file}")" \
  --augment-probability "$(jq -er '.consolidation_stage.augment_probability' "${config_file}")" \
  --eval-steps "$(jq -er '.consolidation_stage.eval_steps' "${config_file}")" \
  --save-steps "$(jq -er '.consolidation_stage.save_steps' "${config_file}")" \
  --metric-for-best-model selection_score \
  --run-test

echo
echo "V104 training complete."
echo "Language candidate: ${language_output}/best"
echo "Consolidated candidate: ${consolidation_output}/best"
