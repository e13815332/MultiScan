#!/bin/bash
set -euo pipefail

# ══════════════════════════════════════════════════════════════════════
# Multiscan Installer
# One-click setup: installs binaries, systemd services, and CLI wrapper
#
# Usage:
#   curl -sL https://github.com/e13815332/multiscan/releases/latest/download/install.sh | bash
#
#   # Or from source:
#   ./install.sh [master|worker <url> [name]|all]
# ══════════════════════════════════════════════════════════════════════

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

VERSION="dev"

# ── Detect platform ──────────────────────────────────────────────────
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64"  ;;
    *)       echo -e "${RED}✗${NC} Unsupported architecture: $ARCH"; exit 1 ;;
esac

OS=$(uname -s)
if [ "$OS" != "Linux" ]; then
    echo -e "${RED}✗${NC} Unsupported OS: $OS (Linux only)"
    exit 1
fi

# ── Check dependencies ────────────────────────────────────────────────
echo -e "${CYAN}═══ Checking dependencies ═══${NC}"

MISSING=""
check_dep() {
    local cmd="$1" pkg="$2"
    if ! command -v "$cmd" &>/dev/null; then
        MISSING="$MISSING $pkg"
        echo -e "  ${YELLOW}✗${NC} $cmd (needs: $pkg)"
    else
        echo -e "  ${GREEN}✓${NC} $cmd"
    fi
}

check_dep "curl"    "curl"
check_dep "systemctl" "systemd"
check_dep "masscan" "masscan"
check_dep "nmap"    "nmap (provides ncat)"

if [ -n "$MISSING" ]; then
    echo ""
    echo -e "${YELLOW}Installing missing dependencies:${NC}$MISSING"
    if command -v apt &>/dev/null; then
        apt update -qq
        for pkg in $MISSING; do
            apt install -y -qq "$pkg" 2>/dev/null || echo "  ! Failed to install $pkg (may already exist)"
        done
    elif command -v yum &>/dev/null; then
        yum install -y -q $MISSING 2>/dev/null || true
    else
        echo -e "${YELLOW}⚠ Package manager not detected. Install manually:${NC}$MISSING"
    fi
fi

# ── Source detection ──────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
HAS_LOCAL_BIN=false

if [ -d "$SCRIPT_DIR/bin" ] && ls "$SCRIPT_DIR/bin"/master-linux-* &>/dev/null 2>&1; then
    HAS_LOCAL_BIN=true
    SOURCE_DIR="$SCRIPT_DIR"
elif [ -d "/root/multiscan/bin" ] && ls /root/multiscan/bin/master-linux-* &>/dev/null 2>&1; then
    HAS_LOCAL_BIN=true
    SOURCE_DIR="/root/multiscan"
fi

if [ "$HAS_LOCAL_BIN" = false ]; then
    echo -e "${YELLOW}⚠ No local binaries found.${NC}"
    read -rp "Download latest release from GitHub? [Y/n] " dl
    if [[ "$dl" =~ ^[Nn]$ ]]; then
        echo "Build locally: make build-all"
        exit 1
    fi

    TMP=$(mktemp -d)
    RELEASE_URL="https://github.com/e13815332/multiscan/releases/latest/download/multiscan-linux-${ARCH}-${VERSION}.tar.gz"
    echo -e "  Downloading from: ${CYAN}$RELEASE_URL${NC}"
    if ! curl -sL "$RELEASE_URL" -o "$TMP/multiscan.tar.gz" || ! tar xzf "$TMP/multiscan.tar.gz" -C "$TMP"; then
        # Try fallback without version
        RELEASE_URL="https://github.com/e13815332/multiscan/releases/latest/download/multiscan-linux-${ARCH}-dev.tar.gz"
        echo -e "  Retrying: ${CYAN}$RELEASE_URL${NC}"
        curl -sL "$RELEASE_URL" -o "$TMP/multiscan.tar.gz"
        tar xzf "$TMP/multiscan.tar.gz" -C "$TMP"
    fi
    SOURCE_DIR="$TMP"
    echo -e "  ${GREEN}✓${NC} Downloaded release"
fi

# ── Parse mode ───────────────────────────────────────────────────────
MODE="${1:-interactive}"
WORKER_MASTER_URL="${2:-ws://localhost:8800/api/worker/ws}"
WORKER_NAME="${3:-$(hostname)}"

# ── Install functions ─────────────────────────────────────────────────
install_cli() {
    echo -e "\n${CYAN}═══ Installing CLI wrapper ═══${NC}"
    cp -f "$SCRIPT_DIR/scripts/multiscan.sh" /usr/local/bin/multiscan 2>/dev/null || {
        # If running from tarball
        if command -v multiscan &>/dev/null; then
            echo "  CLI already installed"
            return
        fi
        cat > /usr/local/bin/multiscan << 'CLIEOF'
#!/bin/bash
# Multiscan CLI — placeholder
# Run the full version from: /root/multiscan/scripts/multiscan.sh
echo "Multiscan — distributed CF proxy scanner"
echo "Run from source: /root/multiscan/scripts/multiscan.sh <command>"
echo ""
echo "Or reinstall with: make install"
CLIEOF
    }
    chmod +x /usr/local/bin/multiscan
    echo -e "  ${GREEN}✓${NC} Installed 'multiscan' command"
}

install_master() {
    echo -e "\n${CYAN}═══ Installing Master ═══${NC}"
    cp -f "$SOURCE_DIR/bin/master-linux-$ARCH" /usr/local/bin/multiscan-master 2>/dev/null || \
    cp -f "$SOURCE_DIR/master" /usr/local/bin/multiscan-master
    chmod +x /usr/local/bin/multiscan-master

    cat > /etc/systemd/system/multiscan-master.service << 'SERVICEEOF'
[Unit]
Description=Multiscan Master — distributed CF proxy scanner controller
Documentation=https://github.com/e13815332/multiscan
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/multiscan-master
Restart=always
RestartSec=5
StartLimitInterval=300
StartLimitBurst=5
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=yes
ProtectHome=read-only

[Install]
WantedBy=multi-user.target
SERVICEEOF

    systemctl daemon-reload
    systemctl enable multiscan-master
    systemctl restart multiscan-master
    echo -e "  ${GREEN}✓${NC} Master running on :8800"
}

install_worker() {
    local master_url="$1"
    local worker_name="$2"

    echo -e "\n${CYAN}═══ Installing Worker ═══${NC}"
    echo -e "  Master URL: ${CYAN}$master_url${NC}"
    echo -e "  Name:       ${CYAN}$worker_name${NC}"

    cp -f "$SOURCE_DIR/bin/worker-linux-$ARCH" /usr/local/bin/multiscan-worker 2>/dev/null || \
    cp -f "$SOURCE_DIR/worker" /usr/local/bin/multiscan-worker
    chmod +x /usr/local/bin/multiscan-worker

    mkdir -p /etc/multiscan
    cat > /etc/multiscan/config << CONFIGEOF
# Multiscan Worker configuration
MASTER_URL="$master_url"
WORKER_NAME="$worker_name"
CONFIGEOF

    cat > /etc/systemd/system/multiscan-worker.service << 'SERVICEEOF'
[Unit]
Description=Multiscan Worker — distributed CF proxy scanner
Documentation=https://github.com/e13815332/multiscan
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/multiscan-worker \
    -master ${MASTER_URL} \
    -name ${WORKER_NAME}
Restart=always
RestartSec=10
StartLimitInterval=300
StartLimitBurst=5
EnvironmentFile=-/etc/multiscan/config
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=yes
ProtectHome=read-only

[Install]
WantedBy=multi-user.target
SERVICEEOF

    systemctl daemon-reload
    systemctl enable multiscan-worker
    systemctl restart multiscan-worker
    echo -e "  ${GREEN}✓${NC} Worker $worker_name running"
}

# ── Uninstall script ─────────────────────────────────────────────────
install_uninstall_script() {
    mkdir -p /root/multiscan/scripts
    cat > /root/multiscan/scripts/uninstall.sh << 'UNINSTALLEOF'
#!/bin/bash
set -euo pipefail
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
echo -e "${YELLOW}Multiscan Uninstaller${NC}"
echo "This will remove: /usr/local/bin/multiscan*, systemd services, /etc/multiscan/"
read -rp "Continue? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || exit 1
for svc in multiscan-master multiscan-worker; do
    systemctl stop "$svc" 2>/dev/null || true
    systemctl disable "$svc" 2>/dev/null || true
done
rm -f /etc/systemd/system/multiscan-master.service /etc/systemd/system/multiscan-worker.service
systemctl daemon-reload
rm -f /usr/local/bin/multiscan-master /usr/local/bin/multiscan-worker /usr/local/bin/multiscan
rm -rf /etc/multiscan
read -rp "Remove source at /root/multiscan? [y/N] " rm_src
[[ "$rm_src" =~ ^[Yy]$ ]] && rm -rf /root/multiscan
echo -e "${GREEN}✓ Uninstalled${NC}"
UNINSTALLEOF
    chmod +x /root/multiscan/scripts/uninstall.sh
    echo -e "  ${GREEN}✓${NC} Uninstall script at /root/multiscan/scripts/uninstall.sh"
}

# ── Firewall ──────────────────────────────────────────────────────────
setup_firewall() {
    echo -e "\n${CYAN}═══ Firewall ═══${NC}"
    if command -v ufw &>/dev/null; then
        ufw allow 8800/tcp 2>/dev/null && echo -e "  ${GREEN}✓${NC} UFW: port 8800 allowed" || echo -e "  ${YELLOW}⚠${NC} UFW config skipped"
    fi
    if command -v firewall-cmd &>/dev/null; then
        firewall-cmd --permanent --add-port=8800/tcp 2>/dev/null || true
        firewall-cmd --reload 2>/dev/null || true
        echo -e "  ${GREEN}✓${NC} firewalld: port 8800 allowed"
    fi
}

# ── Main ──────────────────────────────────────────────────────────────
echo -e "${CYAN}"
echo "╔══════════════════════════════════╗"
echo "║     Multiscan Installer          ║"
echo "╚══════════════════════════════════╝"
echo -e "${NC}"

case "$MODE" in
    master)
        install_cli
        install_master
        setup_firewall
        install_uninstall_script
        ;;
    worker)
        install_cli
        install_worker "$WORKER_MASTER_URL" "$WORKER_NAME"
        install_uninstall_script
        ;;
    all|interactive)
        install_cli
        install_master
        install_worker "$WORKER_MASTER_URL" "$WORKER_NAME"
        setup_firewall
        install_uninstall_script
        ;;
    *)
        echo "Usage: $0 [master|worker <url> [name]|all]"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  Installation complete!                   ║${NC}"
echo -e "${GREEN}║                                          ║${NC}"
echo -e "${GREEN}║  Panel:    http://<host>:8800              ║${NC}"
echo -e "${GREEN}║  CLI:      multiscan status               ║${NC}"
echo -e "${GREEN}║  Logs:     multiscan logs                 ║${NC}"
echo -e "${GREEN}║  Uninstall: multiscan uninstall           ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"

# Clean up temp download
if [ -n "${TMP:-}" ] && [ -d "$TMP" ]; then
    rm -rf "$TMP"
fi
