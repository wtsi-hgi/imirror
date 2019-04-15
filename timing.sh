#!/usr/bin/env bash

# $1   mpistat data
# $2   directory to mirror (no trailing slash)
# $3   irods collection (no trailing slash)

echo "mpistat data file size: $(stat -c "%s" "$1") bytes"

##  Utility Functions ##################################################

fetch_mpistat_inodes() {
  # Filter out inodes from mpistat data (from stdin) of a specific mode
  # matching the given root directory prefix
  local file_mode="$1"
  local local_directory="$2"

  local common_prefix="$(printf "%s\n%s\n" \
                           "$(echo -n "${local_directory}" | base64)" \
                           "$(echo -n "${local_directory}/" | base64)" \
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
  | grep -zE "^${local_directory}(/|$)"
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

## Branch 1: Directory scanning ########################################

SECONDS=0

gunzip -c "$1" \
| fetch_mpistat_inodes d "$2" \
> directories

echo -n "1: Filtered directories in ${SECONDS} seconds; "
echo "$(grep -cF $'\0' directories) directories found"

###

SECONDS=0

find_leaves <directories >leaves

echo -n "1: Found leaves in ${SECONDS} seconds; "
echo "$(grep -cF $'\0' leaves) leaves found"

###

SECONDS=0

swap_roots "$2" "$3" <leaves >swapped_leaves

echo "1: Created minimal collection list in ${SECONDS} seconds"

## Branch 2: File scanning #############################################

SECONDS=0

gunzip -c "$1" \
| fetch_mpistat_inodes f "$2" \
> files

echo -n "2: Filtered files in ${SECONDS} seconds; "
echo "$(grep -cF $'\0' files) files found"

###

SECONDS=0

shuf -z files > shuffled_files

echo "2: Shuffled files in ${SECONDS} seconds"

###

SECONDS=0

FILE_COUNT=$(grep -cF $'\0' shuffled_files)
CHUNK_SIZE=10
CHUNKS=$(( (FILE_COUNT / CHUNK_SIZE) + (FILE_COUNT % CHUNK_SIZE != 0) ))
SUFFIX=${#CHUNKS}

echo "2: Calculated chunk count in ${SECONDS} seconds"

###

TMP=$(mktemp -d)

SECONDS=0

split --lines $CHUNK_SIZE --separator="\0" --suffix-length $SUFFIX --numeric-suffixes=1 \
  shuffled_files "$TMP/"

echo "2: Split into chunks in ${SECONDS} seconds; ${CHUNKS} chunks created"
