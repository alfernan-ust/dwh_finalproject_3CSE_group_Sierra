#!/bin/bash

# Script to restore files from loaded directories back to their original locations
# This is useful for re-running the ETL from scratch

echo "========================================="
echo "Restoring files from loaded directories"
echo "========================================="

# Counter for restored files
restored_count=0

# Find all 'loaded' directories and process them
find dataset -type d -name "loaded" | while read -r loaded_dir; do
    # Skip nested loaded/loaded directories
    if [[ "$loaded_dir" == *"/loaded/loaded"* ]]; then
        echo "Skipping nested directory: $loaded_dir"
        continue
    fi

    # Get parent directory
    parent_dir=$(dirname "$loaded_dir")

    echo ""
    echo "Processing: $loaded_dir"
    echo "Parent dir: $parent_dir"

    # Count files in loaded directory (excluding directories)
    file_count=$(find "$loaded_dir" -maxdepth 1 -type f | wc -l | tr -d ' ')

    if [ "$file_count" -eq 0 ]; then
        echo "  No files to restore in $loaded_dir"
        continue
    fi

    echo "  Found $file_count file(s) to restore"

    # Move all files from loaded back to parent directory
    find "$loaded_dir" -maxdepth 1 -type f | while read -r file; do
        filename=$(basename "$file")
        destination="$parent_dir/$filename"

        # Check if file already exists in parent
        if [ -f "$destination" ]; then
            echo "  ⚠️  File already exists: $destination (skipping)"
        else
            mv "$file" "$destination"
            echo "  ✓ Restored: $filename"
            ((restored_count++))
        fi
    done
done

# Clean up empty nested loaded/loaded directories
echo ""
echo "Cleaning up nested loaded directories..."
find dataset -type d -name "loaded" -path "*/loaded/loaded" -exec rmdir {} \; 2>/dev/null

# Remove empty loaded directories
echo "Removing empty loaded directories..."
find dataset -type d -name "loaded" -empty -delete 2>/dev/null

echo ""
echo "========================================="
echo "Restore complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Review the restored files"
echo "2. Re-run your ETL pipeline"
echo "3. Data will now be appended instead of replaced"
