#!/bin/bash
# Helper script to add OpenAI API key to CMS API service

echo "========================================"
echo "Add OpenAI API Key to CMS API Service"
echo "========================================"
echo
echo "This will enable LLM-powered matching for ambiguous provider names."
echo "Cost: ~\$0.002 per LLM match (very cheap with gpt-4o-mini)"
echo

# Check if user provided key as argument
if [ -z "$1" ]; then
    echo "Usage: ./add_openai_key.sh YOUR_API_KEY"
    echo
    echo "To get your OpenAI API key:"
    echo "  1. Go to https://platform.openai.com/api-keys"
    echo "  2. Create a new key"
    echo "  3. Run: ./add_openai_key.sh sk-proj-YOUR_KEY_HERE"
    echo
    exit 1
fi

API_KEY="$1"

# Validate key format
if [[ ! "$API_KEY" =~ ^sk-proj- ]] && [[ ! "$API_KEY" =~ ^sk- ]]; then
    echo "❌ Invalid API key format. OpenAI keys start with 'sk-' or 'sk-proj-'"
    exit 1
fi

echo "API Key: ${API_KEY:0:10}...${API_KEY: -4}"
echo
echo "This will:"
echo "  1. Backup current service file"
echo "  2. Add OPENAI_API_KEY environment variable"
echo "  3. Reload systemd daemon"
echo "  4. Restart cms-api service"
echo

read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo
echo "Connecting to server..."

# Execute on remote server
ssh root@5.78.148.70 << EOF
    set -e
    
    # Backup
    echo "Creating backup..."
    cp /etc/systemd/system/cms-api.service /etc/systemd/system/cms-api.service.backup
    
    # Check if OPENAI_API_KEY already exists
    if grep -q "OPENAI_API_KEY" /etc/systemd/system/cms-api.service; then
        echo "Updating existing OPENAI_API_KEY..."
        sed -i "s|Environment=OPENAI_API_KEY=.*|Environment=OPENAI_API_KEY=$API_KEY|" /etc/systemd/system/cms-api.service
    else
        echo "Adding OPENAI_API_KEY..."
        # Add after the last Environment= line
        sed -i "/^Environment=/a Environment=OPENAI_API_KEY=$API_KEY" /etc/systemd/system/cms-api.service
    fi
    
    # Reload and restart
    echo "Reloading systemd..."
    systemctl daemon-reload
    
    echo "Restarting cms-api service..."
    systemctl restart cms-api
    
    # Wait a moment
    sleep 2
    
    # Check status
    if systemctl is-active --quiet cms-api; then
        echo "✅ Service restarted successfully"
    else
        echo "❌ Service failed to start. Check logs:"
        echo "   journalctl -u cms-api -n 50"
        exit 1
    fi
    
    echo
    echo "Current environment variables:"
    grep "^Environment=" /etc/systemd/system/cms-api.service | sed 's/OPENAI_API_KEY=.*/OPENAI_API_KEY=sk-proj-***REDACTED***/'
EOF

if [ $? -eq 0 ]; then
    echo
    echo "========================================"
    echo "✅ OpenAI API Key Added Successfully!"
    echo "========================================"
    echo
    echo "LLM matching is now enabled. It will automatically activate"
    echo "as a fallback for ambiguous matches (confidence 0.5-0.7)."
    echo
    echo "Test it:"
    echo "  curl -s 'http://5.78.148.70:8080/match/search?name=John%20Smith&address=Los%20Angeles,%20CA' \\"
    echo "    -H 'X-API-Key: 1bb250cdd582258595a5d2bebd9493f2c74a7999' | jq '.matches[0].llm_reasoning'"
    echo
else
    echo
    echo "❌ Deployment failed. Check the error messages above."
    exit 1
fi
