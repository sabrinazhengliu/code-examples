touch generate_changelog.sh
chmod +x generate_changelog.sh
./generate_changelog.sh


#!/bin/bash

# Default values
VERSION="0.0.0"
INPUT_DIR="."
ROOT_NAME_OVERRIDE=""
HELP_TEXT="Usage: $0 -v <version> [-d <directory>] [-r <root_name>]"

# 1. Parse Command Line Arguments
while getopts "v:d:r:h" opt; do
  case $opt in
    v) VERSION="$OPTARG"
    ;;
    d) INPUT_DIR="$OPTARG"
    ;;
    r) ROOT_NAME_OVERRIDE="$OPTARG"
    ;;
    h) echo "$HELP_TEXT"; exit 0
    ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1
    ;;
  esac
done

# Define the output filename
OUTPUT_FILE="release-${VERSION}.yml"

# Save the original directory so we can write the output file there later
ORIGINAL_DIR=$(pwd)

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Directory '$INPUT_DIR' does not exist."
    exit 1
fi

# Switch context to the target directory
cd "$INPUT_DIR" || exit

# ---------------------------------------------------------
# Determine Root Name and Relative Path
# ---------------------------------------------------------

IS_GIT=false
if git rev-parse --git-dir > /dev/null 2>&1; then
    IS_GIT=true
fi

# 1. Determine Repo Root Name
if [ "$IS_GIT" = true ]; then
    # If in Git, get the actual repo root name
    REPO_ROOT=$(basename "$(git rev-parse --show-toplevel)")
else
    # If not in Git
    if [ -n "$ROOT_NAME_OVERRIDE" ]; then
        # Use the user-supplied argument
        REPO_ROOT="$ROOT_NAME_OVERRIDE"
    else
        # Use the current directory name (default fallback)
        REPO_ROOT=$(basename "$(pwd)")
    fi
fi

# 2. Determine Relative Path (Prefix)
# The path segment between the Root and the File.
if [ "$IS_GIT" = true ]; then
    REL_PATH=$(git rev-parse --show-prefix)
else
    # If not in Git, and we are defaulting to the current directory as root,
    # then the relative path is empty (we are inside the root).
    # However, if a user provided a custom Root Name via -r, 
    # we usually assume the files are strictly inside that root.
    # For simplicity in non-git mode: we assume we are at the base of the scan.
    REL_PATH=""
fi

# ---------------------------------------------------------
# Generate Output
# ---------------------------------------------------------

# Define where to write the file (back in the folder where script was run)
OUTPUT_PATH="${ORIGINAL_DIR}/${OUTPUT_FILE}"

echo "databaseChangeLog" > "$OUTPUT_PATH"
echo "  -id: release-${VERSION}" >> "$OUTPUT_PATH"

TODAY=$(date +%F)
FOUND_COUNT=0

echo "Scanning directory '$(pwd)' for files modified on $TODAY..."

# Loop through files
for file in *; do
    # Check if it is a regular file
    if [ -f "$file" ]; then
        
        # Get modification date
        FILE_DATE=$(date -r "$file" +%F)

        # Compare dates
        if [ "$FILE_DATE" == "$TODAY" ]; then
            echo "  - include:" >> "$OUTPUT_PATH"
            
            # Construct path: changelog/<root>/<rel_path>/<filename>
            FULL_LINE="        changelog/${REPO_ROOT}/${REL_PATH}${file}"
            
            echo "$FULL_LINE" >> "$OUTPUT_PATH"
            ((FOUND_COUNT++))
        fi
    fi
done

echo "--------------------------------"
echo "Success! Found $FOUND_COUNT files."
echo "Output saved to: $OUTPUT_PATH"
