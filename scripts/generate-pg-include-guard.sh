#!/bin/bash

set -euo pipefail

CPP_ONLY_FILE=$1
PG_INCLUDE_DIR=$2

OUTPUT_DIR=$(dirname "${CPP_ONLY_FILE}")

mkdir -p "${OUTPUT_DIR}"

echo "Will generate: $(pwd)/${CPP_ONLY_FILE}"

echo "// Auto-generated file, run 'make update_cpp_guard' to update" > "${CPP_ONLY_FILE}"
echo "#if \\" >> "${CPP_ONLY_FILE}"


grep -R '_H$' "${PG_INCLUDE_DIR}" | grep define | cut -d: -f 2 | cut -d \  -f 2 | sort | while read -r m;
do
    echo "defined($m) || \\";
done >> "${CPP_ONLY_FILE}"

echo "false" >> "${CPP_ONLY_FILE}"
echo "static_assert(false, \"No Postgres header should be included in this file.\");" >> "${CPP_ONLY_FILE}"
echo "#endif" >> "${CPP_ONLY_FILE}"

NB_DEFINED=$(grep defined "${CPP_ONLY_FILE}" | wc -l)
echo "Used $NB_DEFINED macros in ${CPP_ONLY_FILE}"
