#!/bin/bash

#get dir where THIS script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/..


echo "update paths in html files..."

#find all html files in pages dir
for file in pages/*.html; do
    echo "Processing $file..."
    
    # Update stylesheet path
    sed -i 's|href="styles.css"|href="../styles/styles.css"|g' "$file"
    
    # Update config script path
    sed -i 's|src="config.js"|src="../config/config.js"|g' "$file"
    
    # Update internal page links
    sed -i 's|href="login.html"|href="login.html"|g' "$file"
    sed -i 's|href="signup.html"|href="signup.html"|g' "$file"
    sed -i 's|href="dashboard.html"|href="dashboard.html"|g' "$file"
    
    echo "Updated $file"
done

echo "Path updates complete!"