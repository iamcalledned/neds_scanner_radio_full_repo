#!/usr/bin/env bash
set -euo pipefail
trap '' PIPE   # suppress noisy SIGPIPE on shutdown

#############################################################
#   S C A N N E R   R E C O R D E R   (Pulse Sources)
#
#   Loads:
#   - shared runtime settings from config/scanner_recorder.env
#   - town/channel definitions from config/scanner_channels.conf
#
#   - Listens to multiple PulseAudio monitor sources.
#   - For each source:
#     - Captures raw audio with ffmpeg.
#     - Segments audio into separate files based on silence using sox.
#     - Saves segmented .wav files to a staging directory.
#   - A file-watcher for each source:
#     - Moves completed files from staging to the final output dir.
#     - Renames files with a timestamp.
#     - Appends the final file path to a queue for transcription.
#
#############################################################

export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
source "$HOME/venv/bin/activate" 2>/dev/null || true

### =========== Paths / Config Loading ===========
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Allow override by:
#   1) first script arg
#   2) SCANNER_ENV_FILE env var
#   3) repo-level default config
#   4) legacy local .env next to script
CONFIG_FILE_DEFAULT="${REPO_ROOT}/config/scanner_recorder.env"
LEGACY_ENV_FILE="${SCRIPT_DIR}/.env"
ENV_FILE="${1:-${SCANNER_ENV_FILE:-$CONFIG_FILE_DEFAULT}}"

if [[ -f "$ENV_FILE" ]]; then
  echo "[INFO] Loading config from: $ENV_FILE"
  set -o allexport
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +o allexport
elif [[ -f "$LEGACY_ENV_FILE" ]]; then
  echo "[WARN] Config not found at $ENV_FILE"
  echo "[INFO] Falling back to legacy config: $LEGACY_ENV_FILE"
  set -o allexport
  # shellcheck source=/dev/null
  source "$LEGACY_ENV_FILE"
  set +o allexport
else
  echo "[WARN] No config file found."
  echo "[WARN] Tried: $ENV_FILE"
  echo "[WARN] Tried fallback: $LEGACY_ENV_FILE"
  echo "[WARN] Using built-in defaults."
fi

### =========== Config (Shared) ===========
SAMPLE_RATE="${SAMPLE_RATE:-48000}"
ARCHIVE_BASE="${ARCHIVE_BASE:-/home/ned/data/scanner_calls/scanner_archive}"
TRANSCRIBE_QUEUE="${TRANSCRIBE_QUEUE:-/tmp/transcribe_queue.txt}"

# Silence segmentation (sox)
START_DUR="${START_DUR:-0.05}"
STOP_DUR="${STOP_DUR:-3.0}"
THRESH="${THRESH:-0.2%}"
LEAD_PAD="${LEAD_PAD:-0.15}"

# External channel config
CHANNELS_FILE="${CHANNELS_FILE:-${REPO_ROOT}/config/scanner_channels.conf}"

### =========== Logging Setup ===========
LOG_DIR="${LOG_DIR:-${ARCHIVE_BASE}/logs/recorder_logs}"
mkdir -p "$LOG_DIR"

current_log_file() { echo "${LOG_DIR}/recorder_$(date +%Y-%m-%d).log"; }
LOG_FILE="$(current_log_file)"
touch "$LOG_FILE"

echo "stop_dur: ${STOP_DUR}"

# log to terminal and file
exec > >(tee -a "$LOG_FILE") 2>&1

### =========== Midnight log rotation ===========
midnight_rotator() {
  while true; do
    now=$(date +%s)
    midnight=$(date -d 'tomorrow 00:00:00' +%s)
    sleep $(( midnight - now + 1 ))

    NEW_LOG="$(current_log_file)"
    touch "$NEW_LOG"
    LOG_FILE="$NEW_LOG"

    printf "[%s] [INFO ] Log rotated target → %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "$LOG_FILE"
  done
}
midnight_rotator &
ROTATOR_PID=$!

### =========== Colors ===========
S_RESET="$(tput sgr0 2>/dev/null || true)"
C_RED="$(tput setaf 1 2>/dev/null || true)"
C_BLUE="$(tput setaf 4 2>/dev/null || true)"
C_MAG="$(tput setaf 5 2>/dev/null || true)"
C_CYN="$(tput setaf 6 2>/dev/null || true)"
C_GRN="$(tput setaf 2 2>/dev/null || true)"
C_YEL="$(tput setaf 3 2>/dev/null || true)"

### =========== Helper Functions ===========
TS() { date '+%Y-%m-%d %H:%M:%S'; }
log_line(){ printf "[%s] %b%-6s%b %s\n" "$(TS)" "$2" "$1" "$S_RESET" "$3"; }
ok()   { printf "[%s] %b%s%b\n" "$(TS)" "$C_GRN" "$1" "$S_RESET"; }
warn() { printf "[%s] %b%s%b\n" "$(TS)" "$C_YEL" "$1" "$S_RESET"; }
err()  { printf "[%s] %b%s%b\n" "$(TS)" "$C_RED" "$1" "$S_RESET"; }
banner(){ printf "\n┌──────────────────────────────────────────────────────┐\n"; printf "│ %-50s │\n" "$1"; printf "└──────────────────────────────────────────────────────┘\n"; }
need(){ command -v "$1" >/dev/null 2>&1 || { err "Missing dependency: $1"; exit 1; }; }

color_from_name() {
  case "${1^^}" in
    RED)  printf '%s' "$C_RED" ;;
    BLUE) printf '%s' "$C_BLUE" ;;
    MAG)  printf '%s' "$C_MAG" ;;
    CYN)  printf '%s' "$C_CYN" ;;
    GRN)  printf '%s' "$C_GRN" ;;
    YEL)  printf '%s' "$C_YEL" ;;
    *)    printf '%s' "$S_RESET" ;;
  esac
}

#######################################################################
#
#   --- CHANNEL DEFINITIONS ---
#
#   Loaded from CHANNELS_FILE with format:
#
#   tag|Log_Tag|Sink_Description|ColorName|Live_Monitor_Flag
#
#   Example:
#   fd|FD|FD_Audio|RED|1
#   pd|PD|PD_Audio|BLUE|1
#
#######################################################################

CHANNELS_LIST=()
declare -A CH_LOG_TAG
declare -A CH_SINK_DESC
declare -A CH_COLOR
declare -A CH_LIVE_MONITOR

declare -A CH_NULL_SINK
declare -A CH_MONITOR_SRC
declare -A CH_OUT_DIR
declare -A CH_STAGE_DIR
declare -A CH_SUFFIX
declare -A CH_REN_PID
declare -A CH_PIPE_PID

define_channel() {
  local tag="$1"
  local log_tag="$2"
  local sink_desc="$3"
  local color="$4"
  local live_mon="$5"

  CHANNELS_LIST+=( "$tag" )
  CH_LOG_TAG["$tag"]="$log_tag"
  CH_SINK_DESC["$tag"]="$sink_desc"
  CH_COLOR["$tag"]="$color"
  CH_LIVE_MONITOR["$tag"]="$live_mon"

  CH_NULL_SINK["$tag"]="sdr_sink_${tag}"
  CH_MONITOR_SRC["$tag"]="${CH_NULL_SINK[$tag]}.monitor"
  CH_OUT_DIR["$tag"]="${ARCHIVE_BASE}/raw/${tag}"
  CH_STAGE_DIR["$tag"]="${CH_OUT_DIR[$tag]}/.staging"
  CH_SUFFIX["$tag"]="_${tag}"
}

load_channels_from_file() {
  local channels_file="$1"

  if [[ ! -f "$channels_file" ]]; then
    err "Channel config file not found: $channels_file"
    exit 1
  fi

  while IFS='|' read -r tag log_tag sink_desc color_name live_mon || [[ -n "${tag:-}" ]]; do
    # strip CR in case file was edited on Windows
    tag="${tag%$'\r'}"
    log_tag="${log_tag%$'\r'}"
    sink_desc="${sink_desc%$'\r'}"
    color_name="${color_name%$'\r'}"
    live_mon="${live_mon%$'\r'}"

    # skip blank and comment lines
    [[ -z "${tag//[[:space:]]/}" ]] && continue
    [[ "$tag" =~ ^[[:space:]]*# ]] && continue

    define_channel \
      "$tag" \
      "$log_tag" \
      "$sink_desc" \
      "$(color_from_name "$color_name")" \
      "$live_mon"
  done < "$channels_file"

  if [[ "${#CHANNELS_LIST[@]}" -eq 0 ]]; then
    err "Loaded 0 channels from: $channels_file"
    err "Expected format: tag|Log_Tag|Sink_Description|ColorName|Live_Monitor_Flag"
    exit 1
  fi
}

### =========== Load Channel Config ===========
echo "[DEBUG] REPO_ROOT=$REPO_ROOT"
echo "[DEBUG] ENV_FILE=$ENV_FILE"
echo "[DEBUG] CHANNELS_FILE=$CHANNELS_FILE"
[[ -f "$CHANNELS_FILE" ]] && echo "[DEBUG] Channel file exists" || echo "[DEBUG] Channel file missing"

banner "Loading channel config"
load_channels_from_file "$CHANNELS_FILE"
echo "[DEBUG] Loaded channel count: ${#CHANNELS_LIST[@]}"
printf '[DEBUG] Channels: %s\n' "${CHANNELS_LIST[@]}"
ok "Loaded ${#CHANNELS_LIST[@]} channels from $CHANNELS_FILE"

### =========== Preflight ===========
banner "Running Preflight Checks"

for tag in "${CHANNELS_LIST[@]}"; do
  mkdir -p "${CH_OUT_DIR[$tag]}" "${CH_STAGE_DIR[$tag]}"
  ok "Ensured dirs for ${CH_LOG_TAG[$tag]}"
done
touch "$TRANSCRIBE_QUEUE"

need ffmpeg
need sox
need inotifywait
need tee
need pactl
need redis-cli

any_live_monitor=0
for tag in "${CHANNELS_LIST[@]}"; do
  if [[ "${CH_LIVE_MONITOR[$tag]}" == "1" ]]; then
    any_live_monitor=1
    break
  fi
done

if [[ "$any_live_monitor" == "1" ]]; then
  if ! command -v aplay >/dev/null 2>&1; then
    warn "aplay not found; disabling ALL live monitors."
    for tag in "${CHANNELS_LIST[@]}"; do
      CH_LIVE_MONITOR[$tag]=0
    done
  else
    ok "aplay found, live monitoring enabled."
  fi
fi

### =========== Ensure Pulse sinks ===========
banner "Ensuring Pulse sinks"

ensure_sink() {
  local sink="$1" desc="$2"
  if ! pactl list short sinks | awk '{print $2}' | grep -qx "$sink"; then
    pactl load-module module-null-sink "sink_name=$sink" "sink_properties=device.description=$desc" >/dev/null
    ok "Created null sink: $sink ($desc)"
  else
    ok "Null sink present: $sink ($desc)"
  fi
}

for tag in "${CHANNELS_LIST[@]}"; do
  ensure_sink "${CH_NULL_SINK[$tag]}" "${CH_SINK_DESC[$tag]}"
done

### =========== Verify Pulse sources ===========
verify_source() {
  local src="$1" tag="$2"
  if ! pactl list short sources | awk '{print $2}' | grep -qx "$src"; then
    err "Missing source $src for $tag. Check PulseAudio config."
    pactl list short sources
    exit 1
  else
    ok "Verified source: $src"
  fi
}

for tag in "${CHANNELS_LIST[@]}"; do
  verify_source "${CH_MONITOR_SRC[$tag]}" "${CH_LOG_TAG[$tag]}"
done

### =========== File Watchers ===========
banner "File watches (staging → final)"

start_renamer() {
  local tag="$1"
  local log_tag="${CH_LOG_TAG[$tag]}"
  local color="${CH_COLOR[$tag]}"
  local stage="${CH_STAGE_DIR[$tag]}"
  local out="${CH_OUT_DIR[$tag]}"
  local suf="${CH_SUFFIX[$tag]}"

  inotifywait -m "$stage" -e close_write --format '%w%f' \
  | while read -r f; do
      [[ -f "$f" ]] || continue

      local ts dest n now_iso
      ts="$(date +%Y-%m-%d_%H-%M-%S)"
      dest="${out}/rec_${ts}${suf}.wav"
      n=1
      while [[ -e "$dest" ]]; do
        dest="${out}/rec_${ts}${suf}_${n}.wav"
        n=$((n+1))
      done
      mv -f "$f" "$dest"

      (
        redis-cli SET "scanner:${tag}:transmitting" "Y" EX 10 >/dev/null 2>&1
      ) &

      (
        redis-cli XADD scanner:stream:new_call "*" \
          tag "$tag" \
          file "$dest" \
          time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" &>> "$LOG_FILE"
      ) &

      (
        now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        redis-cli SET "scanner:${tag}:latest_time" "$now_iso" >/dev/null 2>&1
      ) &

      log_line "$log_tag" "$color" "saved $(basename "$dest")"
      echo "$dest" >> "$TRANSCRIBE_QUEUE"

      NOW=$(date '+%Y-%m-%d %H:%M:%S.%3N')
      HOUR=$(date '+%Y-%m-%d %H')
      MIN=$(date '+%Y-%m-%d %H:%M')
      STATS_FILE="${ARCHIVE_BASE}/logs/stats.log"
      mkdir -p "$(dirname "$STATS_FILE")"
      printf "%s,%s,%s,%s\n" "$log_tag" "$HOUR" "$MIN" "$NOW" >> "$STATS_FILE"
    done
}

for tag in "${CHANNELS_LIST[@]}"; do
  start_renamer "$tag" &
  CH_REN_PID[$tag]=$!
done
ok "All file watches active."

### =========== Pipelines ===========
banner "Starting Pipelines"

monitor_cmd() {
  echo "aplay -q -t raw -f S16_LE -c 1 -r $SAMPLE_RATE"
}

start_pipeline() {
  local tag="$1"
  local log_tag="${CH_LOG_TAG[$tag]}"
  local color="${CH_COLOR[$tag]}"
  local monitor_src="${CH_MONITOR_SRC[$tag]}"
  local out_dir="${CH_OUT_DIR[$tag]}"
  local stage_dir="${CH_STAGE_DIR[$tag]}"
  local live_mon="${CH_LIVE_MONITOR[$tag]}"

  local mon_cmd
  [[ "$live_mon" == "1" ]] && mon_cmd="$(monitor_cmd)" || mon_cmd="cat >/dev/null"

  log_line "$log_tag" "$color" "source: $monitor_src → $out_dir monitor=$([[ "$live_mon" == "1" ]] && echo ON || echo OFF)"

  (
    ffmpeg -nostats -hide_banner -loglevel error \
      -f pulse -i "$monitor_src" -ac 1 -ar "$SAMPLE_RATE" -f s16le - \
    | tee >(bash -c "$mon_cmd") \
    | sox -V1 -t raw -r "$SAMPLE_RATE" -e signed -b 16 -c 1 - \
          "$stage_dir/rec_.wav" \
          silence -l 1 "$START_DUR" "$THRESH" 1 "$STOP_DUR" "$THRESH" pad "$LEAD_PAD" 0 \
          : newfile : restart
  ) || true &

  CH_PIPE_PID[$tag]=$!
}

for tag in "${CHANNELS_LIST[@]}"; do
  start_pipeline "$tag"
done

### =========== Status ===========
banner "Status"
printf "  %-8s %-38s %-32s %-7s %-7s\n" "TAG" "SOURCE" "OUT DIR" "REN_PID" "PIPE_PID"
printf "  %s\n" "─────────────────────────────────────────────────────────────────────────────────────"
for tag in "${CHANNELS_LIST[@]}"; do
  printf "  %-8s %-38s %-32s %-7s %-7s\n" \
    "${CH_LOG_TAG[$tag]}" \
    "${CH_MONITOR_SRC[$tag]}" \
    "${CH_OUT_DIR[$tag]}" \
    "${CH_REN_PID[$tag]:-}" \
    "${CH_PIPE_PID[$tag]:-}"
done
printf "\n"

### =========== Cleanup ===========
cleanup() {
  banner "Shutdown"
  set +e

  PIDS_TO_KILL=()
  for tag in "${CHANNELS_LIST[@]}"; do
    PIDS_TO_KILL+=( "${CH_REN_PID[$tag]:-}" )
    PIDS_TO_KILL+=( "${CH_PIPE_PID[$tag]:-}" )
  done
  PIDS_TO_KILL+=( "${ROTATOR_PID:-}" )

  warn "Sending INT signal to all child processes..."
  for pid in "${PIDS_TO_KILL[@]}"; do
    [[ -n "$pid" ]] && kill -INT "$pid" 2>/dev/null || true
  done

  sleep 1

  warn "Checking for stubborn processes and sending KILL..."
  for pid in "${PIDS_TO_KILL[@]}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      warn "Killing stubborn PID: $pid"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done

  wait || true
  ok "Clean exit."
}
trap cleanup INT TERM EXIT

ok "All processes running. Waiting for shutdown signal (Ctrl+C)..."
wait