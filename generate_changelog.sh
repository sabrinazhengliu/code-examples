touch generate_changelog.sh
chmod +x generate_changelog.sh
./generate_changelog.sh


#!/bin/bash

# ==========================================
# 1. Initialize Defaults
# ==========================================
VERSION="0.0.0"
INPUT_DIR="."
# Default Root Name is the current directory name
ROOT_NAME=$(basename "$(pwd)")

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
# 3. Calculate Relative Path (Truncation Logic)
# ==========================================

# We look for the ROOT_NAME inside the INPUT_DIR string.
if [[ "$INPUT_DIR" == *"$ROOT_NAME"* ]]; then
    # Logic: Remove everything from the start of the string 
    # UP TO and INCLUDING the ROOT_NAME.
    REL_PATH="${INPUT_DIR#*"$ROOT_NAME"}"
else
    # If ROOT_NAME is not found in the path, we assume the 
    # user provided a path that is already relative to the root.
    REL_PATH="$INPUT_DIR"
fi

# --- Cleanup ---

# 1. Remove leading slash (left over from truncation)
REL_PATH="${REL_PATH#/}"

# 2. Remove leading "./" (if user typed current dir)
REL_PATH="${REL_PATH#./}"

# 3. If REL_PATH is exactly ".", clear it (it means we are at the root)
if [ "$REL_PATH" == "." ]; then
    REL_PATH=""
fi

# 4. Add trailing slash if REL_PATH is not empty and missing one
if [ -n "$REL_PATH" ] && [[ "$REL_PATH" != */ ]]; then
    REL_PATH="${REL_PATH}/"
fi

# ==========================================
# 4. Process Directory
# ==========================================

if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Directory '$INPUT_DIR' does not exist."
    exit 1
fi

# Switch to the target directory
cd "$INPUT_DIR" || exit

# ==========================================
# 5. Generate Output
# ==========================================

OUTPUT_PATH="${ORIGINAL_DIR}/${OUTPUT_FILE}"

echo "databaseChangeLog:" > "$OUTPUT_PATH"
echo "  -id: release-${VERSION}" >> "$OUTPUT_PATH"

TODAY=$(date +%F)
FOUND_COUNT=0

echo "Scanning '$INPUT_DIR'..."
echo "Root Name: $ROOT_NAME"
echo "Calculated Relative Path: $REL_PATH"

# Loop through files
for file in *; do
    if [ -f "$file" ]; then
        
        # Get modification date
        FILE_DATE=$(date -r "$file" +%F)

        if [ "$FILE_DATE" == "$TODAY" ]; then
            echo "  - include:" >> "$OUTPUT_PATH"
            
            # Format: changelog/<root_name>/<truncated_path>/<filename>
            FULL_LINE="        changelog/${ROOT_NAME}/${REL_PATH}${file}"
            
            echo "$FULL_LINE" >> "$OUTPUT_PATH"
            ((FOUND_COUNT++))
        fi
    fi
done

echo "--------------------------------"
echo "Success! Found $FOUND_COUNT files."
echo "Output saved to: $OUTPUT_PATH"
