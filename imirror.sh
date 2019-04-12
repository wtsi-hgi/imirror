#!/usr/bin/env bash

# Mirror directory tree to iRODS
# Christopher Harrison <ch12@sanger.ac.uk>

set -euo pipefail

# Environment variables
# * WORK_DIR    The working directory (default: pwd)
# * CHUNK_SIZE  The maximum number of files in each chunk (default: 10)
# * IPUT_LIMIT  The maximum number of concurrent iputs (default: 20)
# * GROUP       The group under which to submit (default: id -gn)
# * PREP_Q      The queue in which to run the preparation job (default: normal)
# * COPY_Q      The queue in which to run the copy jobs (default: normal)

declare BINARY="$(readlink -fn "$0")"

export WORK_DIR="${WORK_DIR-$(pwd)}"
export GROUP="${GROUP-$(id -gn)}"

declare LOG_DIR="${WORK_DIR}/logs"
declare CHUNK_DIR="${WORK_DIR}/chunks"

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

swap_roots() {
  # Replace the local directory root with the collection root
  local directory_root="$1"
  local collection_root="$2"

  sed -zE "s|^${directory_root}|${collection_root}|"
}

prepare_from_mpistat() {
  # Parse the mpistat file to create the directory structure on iRODS
  # and the list of files, chunked, for the distributed copy submission
  local mpistat_file="$1"
  local directory_root="$(strip_slash "$2")"
  local collection_root="$(strip_slash "$3")"

  local -i file_count
  local -i chunk_size="${CHUNK_SIZE-10}"
  local -i chunks
  local -i chunk_suffix_length

  local -i iput_limit="${IPUT_LIMIT-20}"
  local job_id

  (
    local lock_dir="$(mktemp -d)"
    local temp_dir="$(mktemp -d)"
    trap 'rm -rf ${lock_dir} ${temp_dir}' EXIT

    fetch_mpistat_inodes() {
      # Filter out inodes from mpistat data (from stdin) of a specific mode
      # matching the given root directory prefix
      local file_mode="$1"
      local directory_root="$2"

      local common_prefix="$(printf "%s\n%s\n" \
                               "$(echo -n "${directory_root}" | base64)" \
                               "$(echo -n "${directory_root}/" | base64)" \
                             | sed -e 'N;s/^\(.*\).*\n\1.*$/\1/')"

      awk -v PREFIX="${common_prefix}" -v MODE="${file_mode}" '
        BEGIN { FS = OFS = "\t" }

        # Find files of MODE whose base64 encoding have the correct prefix
        $8 == MODE && $1 ~ "^" PREFIX {
          # n.b., "AA==" is the base64 encoding of \0, so we can stream
          # base64 decoding efficiently, with NULL-delimited output
          print $1 "AA=="
        }
      ' \
      | base64 -di \
      | grep -zE "^${directory_root}(/|$)"
    }

    mirror_directories() {
      local directory_root="$1"
      local collection_root="$2"

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

      fetch_mpistat_inodes d "${directory_root}" \
      | find_leaves \
      | swap_roots "${directory_root}" "${collection_root}" \
      | tee >(xargs -0I% echo imkdir -p % >/dev/null 2>&1)  # TODO Debug
    }

    echo "Reading mpistat data from ${mpistat_file}..." >&2

    # The shuffle in here is to increase the probability of uniformly
    # sized chunks, in terms of total file size. Said probability tends
    # to 1 as the number of the files in each chunk increases
    gunzip -c "${mpistat_file}" \
    | tee >(lock "${lock_dir}/mirror_directories" \
              mirror_directories "${directory_root}" "${collection_root}" \
              > "${temp_dir}/dirs") \
    | fetch_mpistat_inodes f "${directory_root}" \
    | shuf -z \
    > "${temp_dir}/files"

    wait_on_lock "${lock_dir}/mirror_directories"

    file_count=$(grep -cF $'\0' "${temp_dir}/files")
    chunks=$(( (file_count / chunk_size) + (file_count % chunk_size != 0) ))
    chunk_suffix_length=${#chunks}

    # We split here, rather than at the end of the parsing pipeline,
    # because we need to know the total number of files upfront, so we
    # can calculate the suffix length and set the copy job array size
    split --lines "${chunk_size}" --separator="\0" \
          --suffix-length "${chunk_suffix_length}" --numeric-suffixes=1 \
          "${temp_dir}/files" "${CHUNK_DIR}/"

    job_id="$(
      export __IMIRROR_SUFFIX_LENGTH="${chunk_suffix_length}"

      bsub -G "${GROUP}" -q "${COPY_Q-normal}" \
           -J "imirror${RANDOM}[1-${chunks}]%${iput_limit}" \
           -o "${LOG_DIR}/copy.%I.log" -e "${LOG_DIR}/copy.%I.log" \
           -M 1000 -R "select[mem>1000] rusage[mem=1000]" \
           "${BINARY}" __copy "${collection_root}" \
      | grep -Po '(?<=Job <)\d+(?=>)'
    )"

    >&2 cat <<-EOF
				
				Mirrored structure of ${directory_root} into ${collection_root}:
				$(sed -z "s/^/* /" "${temp_dir}/dirs" | xargs -0n1)
				
				${file_count} files to upload, separated into ${chunks} chunks of up to ${chunk_size} files each
				Copy job submitted as Job ${job_id}, throttled to ${iput_limit} concurrent uploads
				EOF
  )
}

upload_chunk_to_irods() {
  local chunk_file="$1"
  local root_collection="$2"

  false
}

main() {
  # TODO Check for invalid arguments
  local mode="$1"

  if [[ "${mode:0:2}" == "__" ]]; then
    shift
  else
    mode="__user"
  fi

  __user() {
    # imirror MPISTAT_FILE LOCAL_DIR IRODS_COLL
    local mpistat_file="$1"
    local local_directory="$2"
    local irods_collection="$3"
    local job_id

    if [[ -e "${LOG_DIR}" ]] || [[ -e "${CHUNK_DIR}" ]]; then
      >&2 echo "Working directory is not clean!"
      exit 1
    fi

    if ils "${irods_collection}" >/dev/null 2>&1; then
      >&2 echo "iRODS collection already exists!"
      exit 1
    fi

    mkdir -p "${LOG_DIR}" "${CHUNK_DIR}"

    job_id="$(bsub -G "${GROUP}" -q "${PREP_Q-normal}" \
                   -o "${LOG_DIR}/prep.log" -e "${LOG_DIR}/prep.log" \
                   -M 5000 -R "select[mem>5000] rusage[mem=5000]" \
                   "${BINARY}" __prepare "${mpistat_file}" "${local_directory}" "${irods_collection}" \
              | grep -Po '(?<=Job <)\d+(?=>)')"

    cat <<-EOF
			Mirroring     ${local_directory} to ${irods_collection}
			mpistat Data  ${mpistat_file}
			
			Preparation job submitted as Job ${job_id}
			EOF
  }

  __prepare() {
    # imirror __prepare MPISTAT_FILE LOCAL_DIR IRODS_COLL
    # TODO This is currently just a trivial wrapper, but it can be
    # extended in the future to allow non-mpistat submissions
    local mpistat_file="$1"
    local local_directory="$2"
    local irods_collection="$3"

    prepare_from_mpistat "${mpistat_file}" "${local_directory}" "${irods_collection}"
  }

  __copy() {
    # imirror __copy IRODS_COLL
    if [[ -z "${LSB_JOBINDEX+x}" ]] || [[ -z "${__IMIRROR_SUFFIX_LENGTH+x}" ]]; then
      >&2 echo "Copy job incorrectly submitted!"
      exit 1
    fi

    local irods_collection="$1"
    local chunk_file="${CHUNK_DIR}/$(printf "%0${__IMIRROR_SUFFIX_LENGTH}d" "${LSB_JOBINDEX}")"

    upload_chunk_to_irods "${chunk_file}" "${irods_collection}"
  }

  case "${mode}" in
    "__user" | "__prepare" | "__copy")
      "${mode}" "$@"
      ;;

    *)
      # TODO Better invalid argument output
      false
      ;;
  esac
}

main "$@"
