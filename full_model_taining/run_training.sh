#!/usr/bin/env bash
set -euo pipefail

training_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
venv_dir="${WHISPER_VENV:-/home/ned/venv_whisper52}"
config_file="${TRAINING_CONFIG:-${training_dir}/training_config.json}"
training_args=()

while (($#)); do
  case "$1" in
    --config)
      if (($# < 2)); then
        echo "--config requires a path" >&2
        exit 2
      fi
      config_file="$2"
      shift 2
      ;;
    --config=*)
      config_file="${1#*=}"
      shift
      ;;
    *)
      training_args+=("$1")
      shift
      ;;
  esac
done

if [[ ! -f "${venv_dir}/bin/activate" || ! -x "${venv_dir}/bin/python" ]]; then
  echo "Whisper virtual environment not found: ${venv_dir}" >&2
  echo "Set WHISPER_VENV to the correct environment directory." >&2
  exit 1
fi

# This is equivalent to the interactive `whisper52` helper in ~/.bashrc.
# shellcheck disable=SC1091
source "${venv_dir}/bin/activate"

echo "Using Whisper environment: ${VIRTUAL_ENV}"
echo "Python: $(command -v python)"

python "${training_dir}/prepare_data.py" --config "${config_file}"
python "${training_dir}/align_long_audio.py" --config "${config_file}"
python "${training_dir}/train_whisper.py" \
  --config "${config_file}" "${training_args[@]}"
