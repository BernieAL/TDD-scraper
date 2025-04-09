#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting log monitor...${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}\n"

# Monitor web server logs
echo -e "${GREEN}=== Web Server Logs ===${NC}"
tail -f "$SCRIPT_DIR/web_server.log" | while read line; do
    echo -e "${BLUE}[Web]${NC} $line"
done

# Monitor the web server log file
echo "Monitoring web server logs..."
tail -f "$SCRIPT_DIR/web_server.log" 