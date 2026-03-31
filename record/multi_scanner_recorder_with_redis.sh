#!/usr/bin/env bash
set -euo pipefail
trap '' PIPE   # suppress noisy SIGPIPE on shutdown

#############################################################
#   S C A N N E R   R E C O R D E R   (Pulse Sources)
#
#   (Refactored to be data-driven)
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

### =========== Load .env ===========
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
if [[ -f "$ENV_FILE" ]]; then
  # Export non-comment, non-blank lines
  set -o allexport
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +o allexport
else
  echo "[WARN] No .env file found at $ENV_FILE — using built-in defaults."
fi

### =========== Config (Shared) ===========
# Values below fall back to defaults if not set by .env
SAMPLE_RATE="${SAMPLE_RATE:-48000}"
ARCHIVE_BASE="${ARCHIVE_BASE:-/home/ned/data/scanner_calls/scanner_archive}"
TRANSCRIBE_QUEUE="${TRANSCRIBE_QUEUE:-/tmp/transcribe_queue.txt}"

# Silence segmentation (sox)
START_DUR="${START_DUR:-0.05}"
STOP_DUR="${STOP_DUR:-3.0}"
THRESH="${THRESH:-0.2%}"
LEAD_PAD="${LEAD_PAD:-0.15}"

### =========== Logging Setup ===========
LOG_DIR="${LOG_DIR:-${ARCHIVE_BASE}/logs/recorder_logs}"
mkdir -p "$LOG_DIR"

# Build today's dated log file path
current_log_file() { echo "${LOG_DIR}/recorder_$(date +%Y-%m-%d).log"; }
LOG_FILE="$(current_log_file)"
touch "$LOG_FILE"

echo "stop_dur: ${STOP_DUR}"

# All stdout/stderr goes to tee: terminal AND today's dated log file.
# The midnight rotator (started below) sends SIGUSR1 when the date rolls over,
# which causes this exec to re-open a fresh file for the new day.
exec > >(tee -a "$LOG_FILE") 2>&1

# --- Midnight log rotation ---
# Runs in the background; sleeps until midnight, then signals the main
# process to reopen its log tee to the new day's file.
midnight_rotator() {
  while true; do
    # Seconds until next midnight
    now=$(date +%s)
    midnight=$(date -d 'tomorrow 00:00:00' +%s)
    sleep $(( midnight - now + 1 ))

    NEW_LOG="$(current_log_file)"
    touch "$NEW_LOG"
    LOG_FILE="$NEW_LOG"

    # Re-exec our stdout/stderr redirect to the new file
    exec > >(tee -a "$LOG_FILE") 2>&1
    printf "[%s] [INFO ] Log rotated → %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "$LOG_FILE"
  done
}
midnight_rotator &
ROTATOR_PID=$!

# --- Colors ---
S_RESET="$(tput sgr0 2>/dev/null || true)"
C_RED="$(tput setaf 1 2>/dev/null || true)"    # FD
C_BLUE="$(tput setaf 4 2>/dev/null || true)"   # PD / bpd
C_MAG="$(tput setaf 5 2>/dev/null || true)"    # mpd
C_CYN="$(tput setaf 6 2>/dev/null || true)"    # mfd/sfd
C_GRN="$(tput setaf 2 2>/dev/null || true)"
C_YEL="$(tput setaf 3 2>/dev/null || true)"

# --- Helper Functions ---
TS() { date '+%Y-%m-%d %H:%M:%S'; }
log_line(){ printf "[%s] %b%-6s%b %s\n" "$(TS)" "$2" "$1" "$S_RESET" "$3"; }
ok()   { printf "[%s] %b%s%b\n" "$(TS)" "$C_GRN" "$1" "$S_RESET"; }
warn() { printf "[%s] %b%s%b\n" "$(TS)" "$C_YEL" "$1" "$S_RESET"; }
err()  { printf "[%s] %b%s%b\n" "$(TS)" "$C_RED" "$1" "$S_RESET"; }
banner(){ printf "\n┌──────────────────────────────────────────────────────┐\n"; printf "│ %-50s │\n" "$1"; printf "└──────────────────────────────────────────────────────┘\n"; }
need(){ command -v "$1" >/dev/null 2>&1 || { err "Missing dependency: $1"; exit 1; }; }






#######################################################################
#
#   --- CHANNEL DEFINITIONS ---
#
#   This is the ONLY section you need to edit to add, remove,
#   or modify channels.
#
#   Just add a new line here. The rest of the script will
#   auto-configure everything.
#
#   Format:
#   define_channel "tag" "Log_Tag" "Sink_Description" "Color" "Live_Monitor_Flag"
#
#   - "tag":     Core ID (lowercase). Used for paths & sink names. (e.g., "fd", "mndfd")
#   - "Log_Tag": Name used in logs (can be different, e.g., "FD")
#   - "Sink_Description": PulseAudio sink description (e.g., "FD_Audio")
#   - "Color":   Log color variable (e.g., "$C_RED")
#   - "Live_Monitor_Flag": 1 to enable, 0 to disable
#
#######################################################################

# --- Arrays to hold all channel data ---
CHANNELS_LIST=() # This array will store the "tag" keys in order
declare -A CH_LOG_TAG
declare -A CH_SINK_DESC
declare -A CH_COLOR
declare -A CH_LIVE_MONITOR

# --- Dynamically generated variables (do not edit) ---
declare -A CH_NULL_SINK
declare -A CH_MONITOR_SRC
declare -A CH_OUT_DIR
declare -A CH_STAGE_DIR
declare -A CH_SUFFIX
declare -A CH_REN_PID
declare -A CH_PIPE_PID

# --- Channel Definition Function ---
define_channel() {
  local tag="$1"
  local log_tag="$2"
  local sink_desc="$3"
  local color="$4"
  local live_mon="$5"

  # Store all properties in arrays, keyed by the 'tag'
  CHANNELS_LIST+=( "$tag" )
  CH_LOG_TAG["$tag"]="$log_tag"
  CH_SINK_DESC["$tag"]="$sink_desc"
  CH_COLOR["$tag"]="$color"
  CH_LIVE_MONITOR["$tag"]="$live_mon"

  # Auto-generate all path and sink variables
  CH_NULL_SINK["$tag"]="sdr_sink_${tag}"
  CH_MONITOR_SRC["$tag"]="${CH_NULL_SINK[$tag]}.monitor"
  CH_OUT_DIR["$tag"]="${ARCHIVE_BASE}/raw/${tag}"
  CH_STAGE_DIR["$tag"]="${CH_OUT_DIR[$tag]}/.staging"
  CH_SUFFIX["$tag"]="_${tag}"
}

# --- Define Your Channels Here ---
## Hopedale PD and FD
define_channel "fd"    "FD"    "FD_Audio"    "$C_RED"   1
define_channel "pd"    "PD"    "PD_Audio"    "$C_BLUE"  1
# Milford PD and FD
define_channel "mpd"   "mpd"   "mpd_Audio"   "$C_MAG"   1
define_channel "mfd"   "mfd"   "mfd_Audio"   "$C_CYN"   1
# Bellingham PD and FD
define_channel "bpd"   "bpd"   "bpd_Audio"   "$C_BLUE"  1
define_channel "bfd"   "bfd"   "bfd_Audio"   "$C_RED"  1
# Mendon PD and FD
define_channel "mndfd" "mndfd" "mndfd_Audio" "$C_MAG"   1
define_channel "mndpd" "mndpd" "mndpd_Audio" "$C_BLUE"   1
# Blackstone PD and FD
define_channel "blkfd" "blkfd" "blkfd_Audio" "$C_RED"   1
define_channel "blkpd" "blkpd" "blkpd_Audio" "$C_BLUE"  1
# Upton PD and FD
define_channel "uptpd" "uptpd" "uptpd_Audio" "$C_BLUE"  1
define_channel "uptfd" "uptfd" "uptfd_Audio" "$C_RED"   1

# Franklin PD and FD
define_channel "frkpd" "frkpd" "frkpd_Audio" "$C_BLUE"  1
define_channel "frkfd" "frkfd" "frkfd_Audio" "$C_RED"   1

# millville PD and FD
define_channel "mllpd" "mllpd" "mllpd_Audio" "$C_BLUE"  1
define_channel "mllfd" "mllfd" "mllfd_Audio" "$C_RED"   1

# test channels
define_channel "test1" "TEST1" "Test1_Audio" "$C_YEL" 0
define_channel "test2" "TEST2" "Test2_Audio" "$C_YEL" 0
define_channel "test3" "TEST3" "Test3_Audio" "$C_YEL" 0
define_channel "test4" "TEST4" "Test4_Audio" "$C_YEL" 0



# To add a new channel, just add a new line:
# define_channel "new" "NEW" "New_Audio" "$C_GRN" 0



### =========== Preflight ===========
banner "Running Preflight Checks"

# --- Create all directories ---
for tag in "${CHANNELS_LIST[@]}"; do
  mkdir -p "${CH_OUT_DIR[$tag]}" "${CH_STAGE_DIR[$tag]}"
  ok "Ensured dirs for ${CH_LOG_TAG[$tag]}"
done
touch "$TRANSCRIBE_QUEUE"

# --- Check dependencies ---
need ffmpeg
need sox
need inotifywait
need tee



# --- Check for aplay if any monitor is live ---
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

# --- Ensure Pulse sinks ---
banner "Ensuring Pulse sinks"
ensure_sink(){
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

# --- Verify Pulse sources ---
verify_source(){
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
      # Check if file still exists (it might be moved by a rapid-fire event)
      [[ -f "$f" ]] || continue

      local ts dest n
      ts="$(date +%Y-%m-%d_%H-%M-%S)"
      dest="${out}/rec_${ts}${suf}.wav"
      n=1
      while [[ -e "$dest" ]]; do
        dest="${out}/rec_${ts}${suf}_${n}.wav"
        n=$((n+1))
      done
      mv -f "$f" "$dest"
      
      # ----------------------------------------------------
      # --- ADD THIS LINE ---
      # Set the "transmitting" flag to "Y" with a 5-second expiry
      # We run it in a background subshell (&) so it doesn't
      # block the loop if redis is slow.
      (redis-cli SET "scanner:${tag}:transmitting" "Y" EX 10 >/dev/null 2>&1) &
      # ----------------------------------------------------


# Publish Redis event with tag and file path
      # Add event to Redis Stream for persistence
# Publish Redis event with tag and file path
      # Add event to Redis Stream for persistence
      (
        redis-cli XADD scanner:stream:new_call "*" \
          tag "$tag" \
          file "$dest" \
          time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" &>> "$LOG_FILE"
      ) &
      # Record the latest call timestamp in UTC ISO format (persistent)
      (
        now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        redis-cli SET "scanner:${tag}:latest_time" "$now_iso" >/dev/null 2>&1
      ) &
      # ----------------------------------------------------


      log_line "$log_tag" "$color" "saved $(basename "$dest")"
      echo "$dest" >> "$TRANSCRIBE_QUEUE"
      # --- Stats Logging ---
      NOW=$(date '+%Y-%m-%d %H:%M:%S.%3N')
      HOUR=$(date '+%Y-%m-%d %H')
      MIN=$(date '+%Y-%m-%d %H:%M')
      STATS_FILE="${ARCHIVE_BASE}/logs/stats.log"
      printf "%s,%s,%s,%s\n" "$log_tag" "$HOUR" "$MIN" "$NOW" >> "$STATS_FILE"
    done
}

for tag in "${CHANNELS_LIST[@]}"; do
  start_renamer "$tag" &
  CH_REN_PID[$tag]=$! # Store the renamer's PID
done
ok "All file watches active."


### =========== Pipelines ===========
banner "Starting Pipelines"

monitor_cmd(){
  echo "aplay -q -t raw -f S16_LE -c 1 -r $SAMPLE_RATE"
}

start_pipeline() {
  local tag="$1"
  # Pull all properties from our central arrays
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
  ) || true & # Run in background and store PID

  CH_PIPE_PID[$tag]=$! # Store the pipeline's PID
}

# --- Start all pipelines ---
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
cleanup(){
  banner "Shutdown"
  set +e
  
  # Build the list of all PIDs from our arrays
  PIDS_TO_KILL=()
  for tag in "${CHANNELS_LIST[@]}"; do
    PIDS_TO_KILL+=( "${CH_REN_PID[$tag]:-}" )
    PIDS_TO_KILL+=( "${CH_PIPE_PID[$tag]:-}" )
  done
  # Include the midnight rotator
  PIDS_TO_KILL+=( "${ROTATOR_PID:-}" )

  warn "Sending INT signal to all child processes..."
  for pid in "${PIDS_TO_KILL[@]}"; do
    [[ -n "$pid" ]] && kill -INT "$pid" 2>/dev/null || true
  done

  # Give them a moment to shut down gracefully
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

# Keep foreground alive and wait for a signal
ok "All processes running. Waiting for shutdown signal (Ctrl+C)..."
wait