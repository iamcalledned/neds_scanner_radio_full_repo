#!/usr/bin/env bash
set -euo pipefail

AGE_DAYS="${AGE_DAYS:-7}"
BACKUP_ROOT="${BACKUP_ROOT:-/mnt/backup_4tb/scanner_archive_backup}"
LOG_FILE="${LOG_FILE:-/home/ned/data/scanner_calls/scanner_archive/logs/archive_sweeper.log}"

SOURCE_DIRS=(
  "/home/ned/data/scanner_calls/scanner_archive/clean"
  "/home/ned/data/scanner_calls/scanner_archive/raw"
)

TOTAL_FOUND=0
TOTAL_MOVED=0
TOTAL_FAILED=0
TOTAL_BYTES=0

timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

log() {
    local msg="$1"
    echo "[$(timestamp)] $msg" | tee -a "$LOG_FILE"
}

human_bytes() {
    local bytes="${1:-0}"
    python3 - <<PY
bytes_val = int($bytes)
units = ["B", "KB", "MB", "GB", "TB"]
size = float(bytes_val)
for unit in units:
    if size < 1024 or unit == units[-1]:
        print(f"{size:.2f} {unit}")
        break
    size /= 1024
PY
}

ensure_mounted() {
    if ! mountpoint -q /mnt/backup_4tb; then
        log "ERROR: /mnt/backup_4tb is not mounted. Aborting."
        exit 1
    fi
}

move_old_files_for_source() {
    local source_dir="$1"
    local source_name dest_root
    local src_found=0
    local src_moved=0
    local src_failed=0
    local src_bytes=0

    source_name="$(basename "$source_dir")"
    dest_root="${BACKUP_ROOT}/${source_name}"

    if [[ ! -d "$source_dir" ]]; then
        log "WARN: Source directory does not exist, skipping: $source_dir"
        return 0
    fi

    mkdir -p "$dest_root"

    log "Scanning source: $source_dir"
    log "Destination   : $dest_root"
    log "Age threshold : $AGE_DAYS days"

    while IFS= read -r -d '' file; do
        local rel_path dest_path dest_parent size
        src_found=$((src_found + 1))

        rel_path="${file#$source_dir/}"
        dest_path="${dest_root}/${rel_path}"
        dest_parent="$(dirname "$dest_path")"

        mkdir -p "$dest_parent"

        if size=$(stat -c %s "$file" 2>/dev/null); then
            :
        else
            size=0
        fi

        if mv "$file" "$dest_path"; then
            src_moved=$((src_moved + 1))
            src_bytes=$((src_bytes + size))
            log "MOVED  : $file -> $dest_path"
        else
            src_failed=$((src_failed + 1))
            log "FAILED : $file"
        fi
    done < <(find "$source_dir" -type f -mtime +"$AGE_DAYS" -print0)

    TOTAL_FOUND=$((TOTAL_FOUND + src_found))
    TOTAL_MOVED=$((TOTAL_MOVED + src_moved))
    TOTAL_FAILED=$((TOTAL_FAILED + src_failed))
    TOTAL_BYTES=$((TOTAL_BYTES + src_bytes))

    log "Summary for $source_name: found=$src_found moved=$src_moved failed=$src_failed bytes=$src_bytes ($(human_bytes "$src_bytes"))"
}

mkdir -p "$(dirname "$LOG_FILE")"
mkdir -p "$BACKUP_ROOT"

START_EPOCH=$(date +%s)

log "============================================================"
log "Starting scanner archive sweep"
log "Backup root : $BACKUP_ROOT"
log "Age days    : $AGE_DAYS"

ensure_mounted

for source_dir in "${SOURCE_DIRS[@]}"; do
    move_old_files_for_source "$source_dir"
done

END_EPOCH=$(date +%s)
DURATION=$((END_EPOCH - START_EPOCH))

log "-------------------- Grand Summary --------------------"
log "Total files found  : $TOTAL_FOUND"
log "Total files moved  : $TOTAL_MOVED"
log "Total files failed : $TOTAL_FAILED"
log "Total bytes moved  : $TOTAL_BYTES ($(human_bytes "$TOTAL_BYTES"))"
log "Duration sec       : $DURATION"
log "Sweep complete"
log "============================================================"