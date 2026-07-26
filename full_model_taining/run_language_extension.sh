#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config_v103_language_extension.json}"

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

language_input="$(jq -er '.language_extension.input_model' "${config_file}")"
language_output="$(jq -er '.language_extension.output_dir' "${config_file}")"
required_wer="$(jq -er '.language_extension.required_wer_below' "${config_file}")"
consolidation_output="$(jq -er '.consolidation.output_dir' "${config_file}")"

echo "Using Whisper environment: ${VIRTUAL_ENV}"
echo "Extending language training from: ${language_input}"

python "${training_dir}/train_whisper.py" \
  --config "${config_file}" \
  --model "${language_input}" \
  --output-dir "${language_output}" \
  --training-stage language \
  --epochs "$(jq -er '.language_extension.epochs' "${config_file}")" \
  --learning-rate "$(jq -er '.language_extension.learning_rate' "${config_file}")" \
  --augment-probability "$(jq -er '.language_extension.augment_probability' "${config_file}")" \
  --eval-steps "$(jq -er '.language_extension.eval_steps' "${config_file}")" \
  --save-steps "$(jq -er '.language_extension.save_steps' "${config_file}")" \
  --no-run-test

language_wer="$(jq -er '.eval_wer' "${language_output}/eval_results.json")"
if ! awk -v result="${language_wer}" -v limit="${required_wer}" 'BEGIN { exit !(result < limit) }'; then
  echo
  echo "Language validation WER ${language_wer} did not beat ${required_wer}."
  echo "Stopping before consolidation. Best model: ${language_output}/best"
  exit 0
fi

echo
echo "Language validation WER improved to ${language_wer}; starting consolidation."

python "${training_dir}/train_whisper.py" \
  --config "${config_file}" \
  --model "${language_output}/best" \
  --output-dir "${consolidation_output}" \
  --training-stage consolidation \
  --epochs "$(jq -er '.consolidation.epochs' "${config_file}")" \
  --learning-rate "$(jq -er '.consolidation.learning_rate' "${config_file}")" \
  --augment-probability "$(jq -er '.consolidation.augment_probability' "${config_file}")" \
  --eval-steps "$(jq -er '.consolidation.eval_steps' "${config_file}")" \
  --save-steps "$(jq -er '.consolidation.save_steps' "${config_file}")" \
  --run-test

echo
echo "V103 language extension complete."
echo "Final Hugging Face model: ${consolidation_output}/best"
