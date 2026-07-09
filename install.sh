#!/usr/bin/env bash
# oboobot install script
# Supports: Linux x86_64, Linux arm64, macOS arm64/x86_64, and Termux
set -e

REPO="https://github.com/oboobotenefiok/oboobot"
BIN_NAME="oboobot"

bold()  { printf "\033[1m%s\033[0m\n" "$*"; }
green() { printf "\033[32m%s\033[0m\n" "$*"; }
dim()   { printf "\033[2m%s\033[0m\n"  "$*"; }
red()   { printf "\033[31m%s\033[0m\n" "$*"; }
die()   { red "Error: $*" >&2; exit 1; }

# Detect platform
OS=$(uname -s)
ARCH=$(uname -m)
TERMUX=false

if [ -d "/data/data/com.termux" ]; then
    TERMUX=true
    OS="Termux"
fi

echo ""
bold "  Installing oboobot — cryptocurrency price monitor"
dim  "  ${REPO}"
echo ""

dim "  Detected: ${OS} / ${ARCH}"

# Install path
if $TERMUX; then
    INSTALL_DIR="$PREFIX/bin"
elif [ -w "/usr/local/bin" ]; then
    INSTALL_DIR="/usr/local/bin"
else
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
fi

# Try download prebuilt binary
RELEASE_URL="${REPO}/releases/latest/download"
BINARY_URL=""

case "${OS}/${ARCH}" in
    Linux/x86_64)         BINARY_URL="${RELEASE_URL}/oboobot-linux-x86_64"  ;;
    Linux/aarch64|Linux/arm64) BINARY_URL="${RELEASE_URL}/oboobot-linux-arm64" ;;
    Darwin/arm64)         BINARY_URL="${RELEASE_URL}/oboobot-macos-arm64"   ;;
    Darwin/x86_64)        BINARY_URL="${RELEASE_URL}/oboobot-macos-x86_64"  ;;
    Termux/*)             BINARY_URL="${RELEASE_URL}/oboobot-termux-aarch64" ;;
    *)                    BINARY_URL="" ;;
esac

DOWNLOADED=false
if [ -n "$BINARY_URL" ]; then
    printf "  Downloading binary… "
    if curl -sSfL "$BINARY_URL" -o "${INSTALL_DIR}/${BIN_NAME}" 2>/dev/null; then
        chmod +x "${INSTALL_DIR}/${BIN_NAME}"
        DOWNLOADED=true
        printf "\033[32m✓\033[0m\n"
    else
        printf "\033[2mno release available, building from source\033[0m\n"
    fi
fi

# Build from source if needed
if ! $DOWNLOADED; then
    dim "  Building from source (requires Rust + cargo)…"
    if ! command -v cargo >/dev/null 2>&1; then
        die "cargo not found. Install Rust from https://rustup.rs and retry."
    fi

    TMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TMP_DIR"' EXIT

    printf "  Cloning repository… "
    git clone --depth=1 "$REPO" "$TMP_DIR/oboobot" >/dev/null 2>&1 || die "git clone failed"
    printf "\033[32m✓\033[0m\n"

    printf "  Compiling (this takes 1–3 minutes)… "
    cd "$TMP_DIR/oboobot"
    cargo build --release --quiet 2>/dev/null || cargo build --release
    cp "target/release/oboobot" "${INSTALL_DIR}/${BIN_NAME}"
    printf "\033[32m✓\033[0m\n"
    cd -
fi

# Verify installation
if ! command -v oboobot >/dev/null 2>&1; then
    echo ""
    dim  "  Binary installed to ${INSTALL_DIR}/oboobot"
    dim  "  Add it to your PATH:"
    printf "    export PATH=\"%s:\$PATH\"\n" "$INSTALL_DIR"
fi

# Create example .env file
ENV_DIR="$HOME/.oboobot"
mkdir -p "$ENV_DIR"
ENV_FILE="$ENV_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
    cat > "$ENV_FILE" << 'EOF'
# CoinGecko API configuration
# Leave empty for keyless (public) access
COINGECKO_API_KEY=
EOF
    green "  ✓ Created example .env file at $ENV_FILE"
fi

echo ""
green "  ✓  oboobot installed successfully"
echo ""

# Run the bot
printf "  Run oboobot now? [Y/n]: "
read -r ANSWER
if [ -z "$ANSWER" ] || [ "$ANSWER" = "y" ] || [ "$ANSWER" = "Y" ]; then
    echo ""
    "${INSTALL_DIR}/${BIN_NAME}"
else
    echo ""
    dim  "  Run 'oboobot' when ready."
fi

echo ""
