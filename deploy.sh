#!/bin/bash
# Distro Dojo - Unified Deployment Script
# Takehome3: Data Quality Assessment
# Follows: unified deployment + unified asset paths

set -e

echo "🛡️ Distro Dojo - Takehome3 Data Quality Deployment"
echo "=================================================="

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# Load unified configuration
CONFIG_FILE="deploy/config.toml"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Unified config not found: $CONFIG_FILE"
    exit 1
fi

echo "✅ Loaded unified deployment config"

# Load secrets (distro dojo - secrets in project)
if [ ! -f "global.env" ]; then
    echo "❌ Secrets not found in project (distro dojo violation)"
    exit 1
fi

source global.env
echo "✅ Secrets loaded from project environment"

# Check prerequisites
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found"
    exit 1
fi

echo "✅ Python environment verified"

# Setup unified asset paths
echo "📁 Setting up unified asset paths..."
mkdir -p logs
mkdir -p data
mkdir -p reports
mkdir -p deploy

# Verify all required files exist (unified asset check)
REQUIRED_FILES=(
    "app.py"
    "requirements.txt"
    "deploy/config.toml"
    "global.env"
)

echo "🔍 Verifying unified asset paths..."
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Missing unified asset: $file"
        exit 1
    fi
done
echo "✅ All unified assets present"

# Install dependencies
echo "📦 Installing unified dependencies..."
pip install -r requirements.txt --quiet || echo "⚠️  Some dependencies may need manual installation"

echo ""
echo "🎯 DEPLOYMENT OPTIONS:"
echo "======================"
echo ""
echo "1. Streamlit Cloud (Recommended):"
echo "   - Repository: https://github.com/anix-lynch/takehome3-data-quality"
echo "   - Main file: app.py"
echo "   - Branch: main"
echo ""
echo "2. Local Development:"
echo "   streamlit run app.py --server.port 8501"
echo ""
echo "3. GitHub Pages (Static):"
echo "   # Convert to static site and deploy"
echo ""
echo "📁 Files ready in project root"
echo "🔐 Secrets available in global.env"
echo ""
echo "✅ Distro Dojo compliant - unified deployment ready!"
