#!/bin/bash
# Setup automated ingestion cron jobs

# Get the absolute path to the project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_PATH="$PROJECT_DIR/.venv/bin/python"
INGESTION_SCRIPT="$PROJECT_DIR/scripts/ingest_all.py"

echo "🔧 Setting up automated CMS data ingestion cron jobs"
echo "📁 Project directory: $PROJECT_DIR"
echo "🐍 Python path: $PYTHON_PATH"
echo "📜 Ingestion script: $INGESTION_SCRIPT"
echo

# Check if Python environment exists
if [ ! -f "$PYTHON_PATH" ]; then
    echo "❌ Python environment not found at $PYTHON_PATH"
    echo "Please run 'poetry install' first"
    exit 1
fi

# Check if ingestion script exists
if [ ! -f "$INGESTION_SCRIPT" ]; then
    echo "❌ Ingestion script not found at $INGESTION_SCRIPT"
    exit 1
fi

# Create cron job entries
CRON_JOBS=(
    # MPFS (annual) - run on January 1st at 2 AM
    "0 2 1 1 * $PYTHON_PATH $INGESTION_SCRIPT --dataset MPFS --year \$(date +%Y)"
    
    # OPPS (quarterly) - run on 1st day of each quarter at 3 AM
    "0 3 1 1,4,7,10 * $PYTHON_PATH $INGESTION_SCRIPT --dataset OPPS --year \$(date +%Y) --quarter \$(echo \$(((\$(date +%-m)-1)/3+1)))"
    
    # ASC (quarterly) - run on 1st day of each quarter at 4 AM
    "0 4 1 1,4,7,10 * $PYTHON_PATH $INGESTION_SCRIPT --dataset ASC --year \$(date +%Y) --quarter \$(echo \$(((\$(date +%-m)-1)/3+1)))"
    
    # CLFS (quarterly) - run on 1st day of each quarter at 5 AM
    "0 5 1 1,4,7,10 * $PYTHON_PATH $INGESTION_SCRIPT --dataset CLFS --year \$(date +%Y) --quarter \$(echo \$(((\$(date +%-m)-1)/3+1)))"
    
    # DMEPOS (quarterly) - run on 1st day of each quarter at 6 AM
    "0 6 1 1,4,7,10 * $PYTHON_PATH $INGESTION_SCRIPT --dataset DMEPOS --year \$(date +%Y) --quarter \$(echo \$(((\$(date +%-m)-1)/3+1)))"
    
    # ASP (quarterly) - run on 1st day of each quarter at 7 AM
    "0 7 1 1,4,7,10 * $PYTHON_PATH $INGESTION_SCRIPT --dataset ASP --year \$(date +%Y) --quarter \$(echo \$(((\$(date +%-m)-1)/3+1)))"
    
    # NADAC (weekly) - run every Monday at 8 AM
    "0 8 * * 1 $PYTHON_PATH $INGESTION_SCRIPT --dataset NADAC --year \$(date +%Y)"
    
    # Full ingestion check (monthly) - run on 15th of each month at 9 AM
    "0 9 15 * * $PYTHON_PATH $INGESTION_SCRIPT --all --year \$(date +%Y)"
)

# Create temporary cron file
TEMP_CRON="/tmp/cms_pricing_cron"

echo "# CMS Pricing API Automated Ingestion" > "$TEMP_CRON"
echo "# Generated on $(date)" >> "$TEMP_CRON"
echo "# Project: $PROJECT_DIR" >> "$TEMP_CRON"
echo "" >> "$TEMP_CRON"

for job in "${CRON_JOBS[@]}"; do
    echo "$job" >> "$TEMP_CRON"
done

echo "📋 Generated cron jobs:"
echo
cat "$TEMP_CRON"
echo

# Ask user if they want to install the cron jobs
read -p "Do you want to install these cron jobs? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Install cron jobs
    crontab "$TEMP_CRON"
    
    if [ $? -eq 0 ]; then
        echo "✅ Cron jobs installed successfully!"
        echo
        echo "📋 Current cron jobs:"
        crontab -l | grep -E "(CMS|ingest)"
        echo
        echo "💡 To view all cron jobs: crontab -l"
        echo "💡 To remove cron jobs: crontab -r"
    else
        echo "❌ Failed to install cron jobs"
        exit 1
    fi
else
    echo "⏭️  Cron jobs not installed"
    echo "💡 You can manually install them later with: crontab $TEMP_CRON"
fi

# Clean up
rm "$TEMP_CRON"

echo
echo "🎯 Next steps:"
echo "   1. Test the ingestion manually:"
echo "      $PYTHON_PATH $INGESTION_SCRIPT --dataset MPFS --year $(date +%Y)"
echo "   2. Check the logs in: $PROJECT_DIR/logs/"
echo "   3. Monitor the data directory: $PROJECT_DIR/data/"
echo "   4. Use the CLI tool: $PYTHON_PATH -m cms_pricing.cli ingestion list-tasks"
