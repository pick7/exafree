#!/bin/bash

# ExaFree Setup Script
# Handles both installation and updates automatically
# Uses uv for Python environment management
# Usage: ./setup.sh

set -e  # Exit on error
PROJECT_ROOT="$(pwd)"

echo "=========================================="
echo "ExaFree Setup Script"
echo "=========================================="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}! $1${NC}"
}

print_step() {
    echo -e "${BLUE}[STEP] $1${NC}"
}

NODE_REQUIREMENT="^20.19.0 || >=22.12.0"

is_admin_panel_disabled() {
    if [ "${DISABLE_ADMIN_PANEL:-}" = "1" ]; then
        return 0
    fi

    if [ -f ".env" ] && grep -Eq '^[[:space:]]*DISABLE_ADMIN_PANEL[[:space:]]*=[[:space:]]*1([[:space:]]*(#.*)?)?$' .env; then
        return 0
    fi

    return 1
}

ensure_clean_worktree() {
    if [ -n "$(git status --porcelain)" ]; then
        print_error "检测到未提交或未跟踪的本地改动，升级前请先处理工作区。"
        git status --short
        exit 1
    fi
}

warn_legacy_static() {
    if [ -d "${PROJECT_ROOT}/static" ]; then
        print_warning "检测到仓库根 legacy static/ 目录；新版源码运行会忽略它，请优先使用 frontend/dist。"
    fi
}

ensure_node_toolchain() {
    if ! command -v node &> /dev/null; then
        print_error "未找到 Node.js。源码部署且启用管理面板时，需要安装 Node.js ${NODE_REQUIREMENT}。Docker 部署不需要宿主机安装 Node.js/npm。"
        exit 1
    fi

    if ! command -v npm &> /dev/null; then
        print_error "未找到 npm。源码部署且启用管理面板时，需要安装 npm。Docker 部署不需要宿主机安装 Node.js/npm。"
        exit 1
    fi

    if ! node -e "const [a,b,c]=process.versions.node.split('.').map(Number); const ok=((a===20&&((b>19)||(b===19&&c>=0)))||(a>22)||(a===22&&((b>12)||(b===12&&c>=0)))); process.exit(ok?0:1)"; then
        print_error "当前 Node.js 版本 $(node -v) 不满足要求（需要 ${NODE_REQUIREMENT}）"
        exit 1
    fi
}

# Check if git is installed
if ! command -v git &> /dev/null; then
    print_error "Git is not installed. Please install git first."
    exit 1
fi

# Step 1: Install or update uv
print_step "Step 1: Installing/Updating uv..."
if ! command -v uv &> /dev/null; then
    print_info "uv not found, installing..."
    # Install uv using pipx or pip
    if command -v pipx &> /dev/null; then
        pipx install uv
    elif command -v pip &> /dev/null; then
        pip install --user uv
    else
        # Fallback: download and install uv binary
        curl -LsSf https://astral.sh/uv/install.sh | sh
        export PATH="$HOME/.local/bin:$PATH"
    fi
    print_success "uv installed successfully"
else
    print_info "Updating uv to latest version..."
    uv pip install --upgrade uv
    print_success "uv updated"
fi
echo ""

# Step 2: Ensure Python 3.11 is available
print_step "Step 2: Ensuring Python 3.11 is available..."
if ! uv python list | grep -q "3.11"; then
    print_info "Python 3.11 not found, installing..."
    uv python install 3.11
    print_success "Python 3.11 installed"
else
    print_success "Python 3.11 is already available"
fi
echo ""

# Step 3: Pull latest code from git
print_step "Step 3: Syncing code from repository..."
ensure_clean_worktree
print_info "Fetching latest changes..."
git fetch origin

print_info "Pulling latest code..."
if git pull --ff-only origin main 2>/dev/null || git pull --ff-only origin master 2>/dev/null; then
    print_success "Code synchronized successfully"
else
    print_info "No remote changes to pull"
fi
echo ""

# Step 4: Setup .env file if it doesn't exist
print_step "Step 4: Checking configuration..."
if [ -f ".env" ]; then
    print_info ".env file exists"
else
    if [ -f ".env.example" ]; then
        cp .env.example .env
        print_success ".env file created from .env.example"
        print_info "Please edit .env and configure your ADMIN_KEY"
    else
        print_error ".env.example not found"
        exit 1
    fi
fi
echo ""

# Step 5: Setup Python virtual environment
print_step "Step 5: Setting up Python environment..."
if [ -d ".venv" ]; then
    print_info "Virtual environment already exists"
else
    print_info "Creating virtual environment with Python 3.11..."
    uv venv --python 3.11 .venv
    print_success "Virtual environment created"
fi
echo ""

# Step 6: Install/Update Python dependencies
print_step "Step 6: Installing Python dependencies..."
print_info "Using uv to install dependencies (this may take a moment)..."
uv pip install --python .venv/bin/python -r requirements.txt --system
print_success "Python dependencies installed"
echo ""

# Step 7: Setup frontend
print_step "Step 7: Setting up frontend..."
if is_admin_panel_disabled; then
    print_info "检测到 DISABLE_ADMIN_PANEL=1，跳过前端构建。"
elif [ -d "frontend" ]; then
    warn_legacy_static
    cd frontend

    ensure_node_toolchain

    print_info "Installing dependencies..."
    npm ci

    print_info "Building frontend..."
    npm run build
    print_success "Frontend built successfully"

    cd ..
else
    print_error "Frontend directory not found. Are you in the project root?"
    exit 1
fi
echo ""

# Step 8: Show completion message
echo "=========================================="
print_success "Setup completed successfully!"
echo "=========================================="
echo ""

if [ -f ".env" ]; then
    print_info "Next steps:"
    echo ""
    echo "  1. Edit .env file if needed:"
    echo "     ${BLUE}nano .env${NC}  or  ${BLUE}vim .env${NC}"
    echo ""
    echo "  2. Start the service:"
    echo "     ${BLUE}uv run python main.py${NC}"
    echo ""
    echo "  3. Access the admin panel:"
    echo "     ${BLUE}http://localhost:7860/${NC}  （源码运行前端资源位于 frontend/dist）"
    echo ""
    print_info "To activate virtual environment later, run:"
    echo "  ${BLUE}source .venv/bin/activate${NC}"
fi
echo ""
