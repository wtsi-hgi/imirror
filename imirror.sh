#!/usr/bin/env bash

# Mirror directory tree as iRODS subcollections
# Christopher Harrison <ch12@sanger.ac.uk>

set -euo pipefail

declare BINARY="$(readlink -fn "$0")"
export WORK_DIR="${WORK_DIR-$(pwd)}"

export LANG="C"

lock() {
  # Acquire a lock for a process
  local lock="$1"

  (( $# > 1 ))
  shift

  touch "${lock}"
  "$@"
  rm -rf "${lock}"
}

wait_on_lock() {
  # Block until a lock is released
  local lock="$1"

  while [[ -e "${lock}" ]]; do
    sleep 1
  done
}

strip_slash() {
  # Strip the trailing slash from a string, if it exists
  local path="$1"

  if [[ "${path: -1}" == "/" ]]; then
    path="${path:0:-1}"
  fi

  echo "${path}"
}

common_prefix() {
  # Calculate the common base64 prefix of a path
  local directory_root="$1"

  printf "%s\n%s\n" \
    "$(echo -n "${directory_root}" | base64)" \
    "$(echo -n "${directory_root}/" | base64)" \
  | sed -e 'N;s/^\(.*\).*\n\1.*$/\1/'
}

fetch_inodes() {
  # Filter out inodes of a specific mode matching the given root
  local file_mode="$1"
  local directory_root="$2"
  local prefix="$(common_prefix "${directory_root}")"

  awk -v PREFIX="${prefix}" -v MODE="${file_mode}" '
    BEGIN { FS = OFS = "\t" }

    # Find directories whose base64 encoding have the correct prefix
    $8 == MODE && $1 ~ "^" PREFIX {
      # n.b., "AA==" is the base64 encoding of \0, so we can stream
      # base64 decoding efficiently
      print $1 "AA=="
    }
  ' \
  | base64 -di \
  | grep -zE "^${directory_root}(/|$)"
}

find_leaves() {
  # Get the leaf nodes of a directory hierarchy
  sort -z \
  | awk '
    BEGIN { RS = ORS = "\0" }

    $0 !~ "^" previous "/" { print previous }
    { previous = $0 }

    END { print previous }
  '
}

swap_roots() {
  # Replace the local directory root with the collection root
  local directory_root="$1"
  local collection_root="$2"

  sed -zE "s|^${directory_root}|${collection_root}|"
}

main() {
  local directory_root="$(strip_slash "$1")"
  local collection_root="$(strip_slash "$2")"

  local -i file_count
  local -i chunk_size="${CHUNK_SIZE-10}"
  local -i chunks
  local -i chunk_suffix_length

  local lock_dir="$(mktemp -d)"
  local temp_dir="$(mktemp -d)"

  mirror_directories() {
    local directory_root="$1"
    local collection_root="$2"

    fetch_inodes d "${directory_root}" \
    | find_leaves \
    | swap_roots "${directory_root}" "${collection_root}" \
    | tee >(xargs -0I% echo imkdir -p % >/dev/null 2>&1)  # TODO Debug
  }

  echo "Reading mpistat data from stdin..." >&2

  tee >(lock "${lock_dir}/mirror_directories" \
          mirror_directories "${directory_root}" "${collection_root}" \
          > "${temp_dir}/dirs") \
  | fetch_inodes f "${directory_root}" \
  | shuf -z \
  > "${temp_dir}/files"

  wait_on_lock "${lock_dir}/mirror_directories"

  file_count=$(grep -cF $'\0' "${temp_dir}/files")
  chunks=$(( (file_count / chunk_size) + (file_count % chunk_size != 0) ))
  chunk_suffix_length=${#chunks}

  # TODO Move state directory creation to controller
  mkdir -p "${WORK_DIR}/chunks" "${WORK_DIR}/logs"
  split --lines "${chunk_size}" --separator="\0" \
        --suffix-length "${chunk_suffix_length}" --numeric-suffixes=1 \
        "${temp_dir}/files" "${WORK_DIR}/chunks/"

  (
    # Report mirroring of directories and chunking
    echo
    echo "Mirrored ${directory_root} into ${collection_root}:"
    sed -z "s/^/* /" "${temp_dir}/dirs" | xargs -0n1

    echo
    echo "${file_count} files to upload, separated into ${chunks} chunks of up to ${chunk_size} files"
  ) >&2
}

main "$@"
