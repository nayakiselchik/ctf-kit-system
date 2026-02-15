#!/usr/bin/env bash
set -euo pipefail

TRAFFIC_DIR="${TRAFFIC_DIR:-/traffic}"
OUT_DIR="${OUT_DIR:-/zeeklogs}"
STATE_DIR="${STATE_DIR:-/state}"

mkdir -p "${OUT_DIR}" "${STATE_DIR}"
touch "${STATE_DIR}/processed.list"

process_pcap() {
  local f="$1"
  local base
  base="$(basename "$f")"
  local stamp="${base%.*}"
  local out="${OUT_DIR}/${stamp}"
  mkdir -p "${out}"

  # skip if already processed
  if grep -Fxq "$base" "${STATE_DIR}/processed.list"; then
    return
  fi

  # zeek offline processing; logs -> out dir
  zeek -C -r "$f" "Log::default_logdir=${out}" || true

  echo "$base" >> "${STATE_DIR}/processed.list"
}

# process existing
shopt -s nullglob
for f in "${TRAFFIC_DIR}"/*.pcap "${TRAFFIC_DIR}"/*.pcapng; do
  # arkime краще з pcap, але zeek може і pcapng; то може залишаємо як є
  process_pcap "$f"
done

# watching new files
inotifywait -m -e close_write,move --format '%f' "${TRAFFIC_DIR}" | while read -r name; do
  case "$name" in
    *.pcap|*.pcapng)
      process_pcap "${TRAFFIC_DIR}/${name}"
      ;;
  esac
done
