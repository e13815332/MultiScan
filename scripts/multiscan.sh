#!/bin/bash
# ────────────────────────────────────────────────────────────────────
# Multiscan CLI — quick commands for multiscan master/worker
# Usage:
#   multiscan                    → status overview
#   multiscan start              → start both services
#   multiscan stop               → stop both services
#   multiscan restart            → restart both services
#   multiscan status             → service status
#   multiscan logs   [master|worker]  → tail logs
#   multiscan update             → self-update (download latest release)
#   multiscan uninstall          → run uninstall script
#   multiscan version            → show version
# ────────────────────────────────────────────────────────────────────

CMD="${1:-status}"
shift 2>/dev/null || true

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

VERSION="dev"

check_service() {
    local name="$1"
    local label="$2"
    if systemctl is-active --quiet "$name" 2>/dev/null; then
        echo -e "  ${GREEN}●${NC} $label (active)"
    elif systemctl is-enabled --quiet "$name" 2>/dev/null; then
        echo -e "  ${YELLOW}●${NC} $label (inactive, enabled)"
    else
        echo -e "  ${RED}✗${NC} $label (not installed)"
    fi
}

show_status() {
    echo -e "${CYAN}╔══ Multiscan Status ══╗${NC}"
    check_service "multiscan-master"  "Master  (:8800)"
    check_service "multiscan-worker"  "Worker"
    echo ""
    if systemctl is-active --quiet multiscan-master 2>/dev/null; then
        local wc
        wc=$(curl -s http://localhost:8800/api/worker/list 2>/dev/null | grep -c '"online":true' 2>/dev/null || echo "?")
        echo -e "  Workers online: ${CYAN}$wc${NC}"
        curl -s http://localhost:8800/health 2>/dev/null | grep -q ok && echo -e "  Panel:          ${GREEN}http://localhost:8800${NC}"
    fi
}

case "$CMD" in
    start)
        echo "Starting multiscan..."
        systemctl start multiscan-master 2>/dev/null && echo "  ✓ Master started" || echo "  ! Master not installed"
        systemctl start multiscan-worker 2>/dev/null && echo "  ✓ Worker started" || echo "  ! Worker not installed"
        ;;
    stop)
        echo "Stopping multiscan..."
        systemctl stop multiscan-worker 2>/dev/null || true
        systemctl stop multiscan-master 2>/dev/null || true
        echo "  ✓ Stopped"
        ;;
    restart)
        echo "Restarting multiscan..."
        systemctl restart multiscan-worker 2>/dev/null || true
        systemctl restart multiscan-master 2>/dev/null || true
        echo "  ✓ Restarted"
        ;;
    status)
        show_status
        ;;
    logs)
        unit="${1:-}"
        if [ "$unit" = "master" ] || [ "$unit" = "" ]; then
            echo "=== Master logs ==="
            journalctl -u multiscan-master --no-pager -n 30
            echo ""
        fi
        if [ "$unit" = "worker" ] || [ "$unit" = "" ]; then
            echo "=== Worker logs ==="
            journalctl -u multiscan-worker --no-pager -n 30
        fi
        ;;
    update)
        echo "Downloading latest multiscan release..."
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64)  ARCH="amd64" ;;
            aarch64) ARCH="arm64"  ;;
            *)       echo "Unsupported arch: $ARCH"; exit 1 ;;
        esac
        URL="https://github.com/e13815332/multiscan/releases/latest/download/multiscan-linux-${ARCH}-dev.tar.gz"
        TMP=$(mktemp -d)
        if curl -sL "$URL" -o "$TMP/multiscan.tar.gz" && tar xzf "$TMP/multiscan.tar.gz" -C "$TMP"; then
            systemctl stop multiscan-worker 2>/dev/null || true
            systemctl stop multiscan-master 2>/dev/null || true
            cp -f "$TMP/master" /usr/local/bin/multiscan-master 2>/dev/null && echo "  ✓ Updated master binary"
            cp -f "$TMP/worker" /usr/local/bin/multiscan-worker 2>/dev/null && echo "  ✓ Updated worker binary"
            systemctl start multiscan-master 2>/dev/null || true
            systemctl start multiscan-worker 2>/dev/null || true
            echo "  ✓ Update complete"
        else
            echo "  ! Failed to download release. Build from source: make build-all"
        fi
        rm -rf "$TMP"
        ;;
    uninstall)
        SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
        if [ -f "$SCRIPT_DIR/uninstall.sh" ]; then
            bash "$SCRIPT_DIR/uninstall.sh"
        elif [ -f /root/multiscan/scripts/uninstall.sh ]; then
            bash /root/multiscan/scripts/uninstall.sh
        else
            echo "uninstall.sh not found"
            exit 1
        fi
        ;;
    version)
        echo "Multiscan $VERSION"
        ;;
    *)
        echo -e "Multiscan — distributed CF proxy scanner"
        echo ""
        echo "Usage:"
        echo "  multiscan                    Show status"
        echo "  multiscan start|stop|restart Control services"
        echo "  multiscan logs   [m|w]       Tail logs"
        echo "  multiscan update             Download latest release"
        echo "  multiscan uninstall          Remove all components"
        echo "  multiscan version            Show version"
        ;;
esac
