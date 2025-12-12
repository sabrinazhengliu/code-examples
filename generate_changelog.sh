touch generate_changelog.sh
chmod +x generate_changelog.sh
./generate_changelog.sh


#!/bin/bash

# ==========================================
# 1. Initialize Defaults
# ==========================================
VERSION="0.0.0"          # Default version
INPUT_DIR="."            # Default input directory
ROOT_NAME="default"      # Placeholder
USER_PROVIDED_ROOT=false # Flag

HELP_TEXT="Usage: $0 [-v <version>] [-d <directory>] [-r <root_name>]"

# ==========================================
# 2. Parse Command Line Arguments
# ==========================================
while getopts "v:d:r:h" opt; do
  case $opt in
    v) VERSION="$OPTARG"
    ;;
    d) INPUT_DIR="$OPTARG"
    ;;
    r) ROOT_NAME="$OPTARG"
       USER_PROVIDED_ROOT=true
    ;;
    h) echo "$HELP_TEXT"; exit 0
    ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1
    ;;
  esac
done

# Output filename
OUTPUT_FILE="release-${VERSION}.yml"
ORIGINAL_DIR=$(pwd)

# ==========================================
# 3. Calculate Relative Path (String Logic)
# ==========================================
# We derive the relative path from the string you passed to -d.
# This happens BEFORE we cd into the directory.

# Remove leading "./" if user typed it (e.g., ./src -> src)
REL_PATH="${INPUT_DIR#./}"

# If the path is just "." (current dir), clear it so we don't print "changelog/root/./file"
if [ "$REL_PATH" == "." ]; then
    REL_PATH=""
fi

# If REL_PATH is not empty and doesn't end with a slash, add one
if [ -n "$REL_PATH" ] && [[ "$REL_PATH" != */ ]]; then
    REL_PATH="${REL_PATH}/"
fi

# ==========================================
# 4. Setup Directory
# ==========================================

if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Directory '$INPUT_DIR' does not exist."
    exit 1
fi

# Switch to the target directory to scan files
cd "$INPUT_DIR" || exit

# ==========================================
# 5. Determine Root Name
# ==========================================

# If user didn't provide -r, use the current folder name
if [ "$USER_PROVIDED_ROOT" = false ]; then
    ROOT_NAME=$(basename "$(pwd)")
fi

# ==========================================
# 6. Generate Output
# ==========================================

OUTPUT_PATH="${ORIGINAL_DIR}/${OUTPUT_FILE}"

echo "databaseChangeLog" > "$OUTPUT_PATH"
echo "  -id: release-${VERSION}" >> "$OUTPUT_PATH"

TODAY=$(date +%F)
FOUND_COUNT=0

echo "Scanning '$INPUT_DIR'..."
echo "Using Root Name: $ROOT_NAME"
echo "Using Relative Path prefix: $REL_PATH"

# Loop through files
for file in *; do
    if [ -f "$file" ]; then
        
        # Get modification date
        FILE_DATE=$(date -r "$file" +%F)

        if [ "$FILE_DATE" == "$TODAY" ]; then
            echo "  - include:" >> "$OUTPUT_PATH"
            
            # Format: changelog/<root_name>/<rel_path_from_args>/<filename>
            FULL_LINE="        changelog/${ROOT_NAME}/${REL_PATH}${file}"
            
            echo "$FULL_LINE" >> "$OUTPUT_PATH"
            ((FOUND_COUNT++))
        fi
    fi
done

echo "--------------------------------"
echo "Success! Found $FOUND_COUNT files."
echo "Output saved to: $OUTPUT_PATH"
