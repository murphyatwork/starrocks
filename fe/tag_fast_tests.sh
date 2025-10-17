#!/bin/bash

# Script to add @Tag("fast") annotation to identified fast test files
# This script processes the list of test files that meet our criteria

cd /Users/ming/Code/starrocks/fe/fe-core/src/test/java

# Read the list of files to tag
while IFS= read -r file; do
    if [ -f "$file" ]; then
        echo "Processing: $file"
        
        # Check if file already has @Tag annotation
        if grep -q "@Tag" "$file"; then
            echo "  Skipping: Already has @Tag annotation"
            continue
        fi
        
        # Check if file has JUnit imports
        if ! grep -q "import org.junit" "$file"; then
            echo "  Skipping: No JUnit imports found"
            continue
        fi
        
        # Create a temporary file
        temp_file=$(mktemp)
        
        # Process the file
        awk '
        BEGIN { tag_import_added = 0; tag_annotation_added = 0; class_found = 0; last_junit_import = 0 }
        
        # Track the last JUnit import line
        /^import org\.junit/ {
            last_junit_import = NR
            print $0
            next
        }
        
        # Add Tag import after the last JUnit import
        NR == last_junit_import + 1 && !tag_import_added && last_junit_import > 0 {
            if (!/import org\.junit\.api\.Tag/) {
                print "import org.junit.jupiter.api.Tag;"
                tag_import_added = 1
            }
        }
        
        # Add @Tag("fast") annotation before class declaration
        /^public class.*Test/ && !tag_annotation_added && !class_found {
            print "@Tag(\"fast\")"
            print $0
            tag_annotation_added = 1
            class_found = 1
            next
        }
        
        # Print all other lines
        { print $0 }
        ' "$file" > "$temp_file"
        
        # Replace original file if changes were made
        if ! cmp -s "$file" "$temp_file"; then
            mv "$temp_file" "$file"
            echo "  Updated: Added @Tag(\"fast\") annotation"
        else
            rm "$temp_file"
            echo "  No changes needed"
        fi
    else
        echo "  Warning: File not found: $file"
    fi
done < /tmp/final_fast_tests.txt

echo "Tagging complete!"
