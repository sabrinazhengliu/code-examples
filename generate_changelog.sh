touch generate_changelog.sh
chmod +x generate_changelog.sh
./generate_changelog.sh



#!/bin/bash

# ==========================================
# 1. Initialize Defaults
# ==========================================
VERSION="0.0.0"          # Default version
INPUT_DIR="."            # Default input directory
ROOT_NAME="default"      # Placeholder (will be set to folder name if not provided)

# We use a flag to track if the user actually passed a root name
USER_PROVIDED_ROOT=false

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

# Define output filename
OUTPUT_FILE="release-${VERSION}.yml"

# Save the original directory to write the output file there later
ORIGINAL_DIR=$(pwd)

# ==========================================
# 3. Setup Directory
# ==========================================

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Directory '$INPUT_DIR' does not exist."
    exit 1
fi

# Switch context to the target directory
cd "$INPUT_DIR" || exit

# ==========================================
# 4. Determine Root Name and Path
# ==========================================

# --- Logic for Root Name (Static/Manual only) ---
# If the user did NOT provide -r, default to current directory name
if [ "$USER_PROVIDED_ROOT" = false ]; then
    ROOT_NAME=$(basename "$(pwd)")
fi

# --- Logic for Relative Path ---
# We still check git for the *path* structure to keep the changelog path accurate,
# but we do not use git for the root name.
REL_PATH=""
if git rev-parse --git-dir > /dev/null 2>&1; then
    # If inside git, get path relative to the git root
    REL_PATH=$(git rev-parse --show-prefix)
fi

# ==========================================
# 5. Generate Output File
# ==========================================

OUTPUT_PATH="${ORIGINAL_DIR}/${OUTPUT_FILE}"

# Write Header
echo "databaseChangeLog" > "$OUTPUT_PATH"
echo "  -id: release-${VERSION}" >> "$OUTPUT_PATH"

TODAY=$(date +%F)
FOUND_COUNT=0

echo "Scanning '$INPUT_DIR'..."
echo "Using Root Name: $ROOT_NAME"

# Loop through files
for file in *; do
    if [ -f "$file" ]; then
        
        # Get modification date
        FILE_DATE=$(date -r "$file" +%F)

        if [ "$FILE_DATE" == "$TODAY" ]; then
            echo "  - include:" >> "$OUTPUT_PATH"
            
            # Format: changelog/<root_name>/<rel_path>/<filename>
            FULL_LINE="        changelog/${ROOT_NAME}/${REL_PATH}${file}"
            
            echo "$FULL_LINE" >> "$OUTPUT_PATH"
            ((FOUND_COUNT++))
        fi
    fi
done

echo "--------------------------------"
echo "Success! Found $FOUND_COUNT files."
echo "Output saved to: $OUTPUT_PATH"
