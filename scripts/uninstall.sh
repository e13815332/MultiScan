#!/bin/bash
set -euo pipefail

# ──────────────────────────────────────────────────
# Multiscan Uninstaller
# Removes all multiscan components from the system
# ──────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Multiscan Uninstaller${NC}"
echo "This will remove:"
echo "  • /usr/local/bin/multiscan* (binary + CLI wrapper)"
echo "  • /etc/systemd/system/multiscan-*.service"
echo "  • /etc/multiscan/ (config)"
echo "  • /root/multiscan/ (source code)"
echo ""

read -rp "Continue? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || exit 1

echo ""
echo "==> Stopping services..."
for svc in multiscan-master multiscan-worker; do
    if systemctl is-active --quiet "$svc" 2>/dev/null; then
        systemctl stop "$svc" || true
        echo "  ✓ Stopped $svc"
    fi
done

echo "==> Disabling services..."
for svc in multiscan-master multiscan-worker; do
    if systemctl is-enabled --quiet "$svc" 2>/dev/null; then
        systemctl disable "$svc" || true
        echo "  ✓ Disabled $svc"
    fi
done

echo "==> Removing systemd unit files..."
rm -f /etc/systemd/system/multiscan-master.service
rm -f /etc/systemd/system/multiscan-worker.service
systemctl daemon-reload
echo "  ✓ Removed unit files"

echo "==> Removing binaries..."
rm -f /usr/local/bin/multiscan-master
rm -f /usr/local/bin/multiscan-worker
rm -f /usr/local/bin/multiscan
echo "  ✓ Removed binaries"

echo "==> Removing config..."
rm -rf /etc/multiscan
echo "  ✓ Removed config"

echo ""
read -rp "Remove source code at /root/multiscan? [y/N] " rm_src
if [[ "$rm_src" =~ ^[Yy]$ ]]; then
    rm -rf /root/multiscan
    echo "  ✓ Removed source"
fi

echo ""
echo -e "${GREEN}✓ Multiscan uninstalled successfully${NC}"
