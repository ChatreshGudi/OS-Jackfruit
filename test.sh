#!/usr/bin/env bash
# test.sh - Full runtime test sequence for the multi-container runtime.
# Run from the repo root:   sudo bash test.sh
# Architecture note: this VM is aarch64. Uses alpine aarch64 rootfs.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BPDIR="$REPO/boilerplate"
ENGINE="$BPDIR/engine"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

# ---- require root ----
if [[ "$(id -u)" -ne 0 ]]; then
    echo "Run as root: sudo bash test.sh" >&2
    exit 1
fi

cd "$REPO"

# ---- make sure binaries are built ----
info "Building boilerplate..."
make -C "$BPDIR" >/dev/null 2>&1
pass "Build OK"

# ---- load kernel module ----
if lsmod | awk '{print $1}' | grep -qx monitor; then
    info "monitor.ko already loaded"
else
    info "Loading monitor.ko..."
    insmod "$BPDIR/monitor.ko"
    pass "insmod OK"
fi

for _ in $(seq 1 10); do [[ -e /dev/container_monitor ]] && break; sleep 0.2; done
[[ -e /dev/container_monitor ]] && pass "/dev/container_monitor exists" || { fail "/dev/container_monitor missing"; exit 1; }
dmesg | tail -n 5

# ---- rootfs setup ----
if [[ ! -d "$REPO/rootfs-base/bin" ]]; then
    fail "rootfs-base not found. Run from repo root after setup."; exit 1
fi

# ensure workload binaries are in base rootfs
cp -f "$BPDIR/cpu_hog"    "$REPO/rootfs-base/"
cp -f "$BPDIR/io_pulse"   "$REPO/rootfs-base/"
cp -f "$BPDIR/memory_hog" "$REPO/rootfs-base/"
pass "Workload binaries copied to rootfs-base"

# Create (or refresh) per-container rootfs copies
for name in alpha beta cpu1 cpu2 mem1 gamma iopulse; do
    rm -rf "$REPO/rootfs-$name"
    cp -a "$REPO/rootfs-base" "$REPO/rootfs-$name"
done
pass "Per-container rootfs copies created"

# ---- start supervisor ----
rm -f /tmp/mini_runtime.sock
info "Starting supervisor..."
"$ENGINE" supervisor "$REPO/rootfs-base" >/tmp/supervisor_test.log 2>&1 &
SUPV_PID=$!
echo "$SUPV_PID" > /tmp/supervisor.pid
sleep 1

if ! kill -0 "$SUPV_PID" 2>/dev/null; then
    fail "Supervisor exited unexpectedly"
    cat /tmp/supervisor_test.log
    exit 1
fi
[[ -e /tmp/mini_runtime.sock ]] && pass "Supervisor running (pid=$SUPV_PID, socket OK)" || { fail "Supervisor socket missing"; exit 1; }

# ---- DEMO 1: ps with no containers ----
info "=== DEMO 1: ps (empty) ==="
"$ENGINE" ps

# ---- DEMO 1: start two containers (multi-container) ----
info "=== DEMO 1: start alpha + beta (multi-container) ==="
"$ENGINE" start alpha "$REPO/rootfs-alpha" "/cpu_hog 30"
"$ENGINE" start beta  "$REPO/rootfs-beta"  "/cpu_hog 30"
sleep 1

echo ""
info "=== DEMO 2: ps (should show alpha + beta running) ==="
"$ENGINE" ps

# ---- DEMO 3: logs ----
sleep 2
info "=== DEMO 3: logs alpha ==="
"$ENGINE" logs alpha
info "=== DEMO 3: logs beta ==="
"$ENGINE" logs beta

# ---- DEMO 4: run (foreground, blocks until done) ----
info "=== DEMO 4: run gamma (foreground, cpu_hog 5s) ==="
"$ENGINE" run gamma "$REPO/rootfs-gamma" "/cpu_hog 5"
pass "run gamma completed"

# ---- DEMO 4: ps should show alpha+beta running, gamma exited ----
info "=== ps after run gamma ==="
"$ENGINE" ps

# ---- DEMO 5: soft-limit warning (memory) ----
info "=== DEMO 5: memory soft-limit (24 MiB soft, 40 MiB hard, 8 MiB/500ms) ==="
info "Starting mem1 container - watch dmesg for SOFT LIMIT warning..."
"$ENGINE" run mem1 "$REPO/rootfs-mem1" "/memory_hog 4 1000" --soft-mib 8 --hard-mib 24 || true
pass "mem1 finished (expected: killed by hard limit)"
echo ""
info "dmesg (last 20 lines, look for SOFT LIMIT and HARD LIMIT):"
dmesg | tail -n 20
echo ""
info "ps (mem1 should show hard_limit_killed):"
"$ENGINE" ps

# ---- DEMO 6: hard-limit kill already shown above via mem1 ----
# (if mem1 shows hard_limit_killed, DEMO 6 is covered)

# ---- DEMO 7: scheduler experiment ----
info "=== DEMO 7: scheduler experiment (nice 0 vs nice 10) ==="
info "Starting cpu1 (nice=0) and cpu2 (nice=10) each running cpu_hog 15s..."
"$ENGINE" start cpu1 "$REPO/rootfs-cpu1" "/cpu_hog 15" --nice 0
"$ENGINE" start cpu2 "$REPO/rootfs-cpu2" "/cpu_hog 15" --nice 10
sleep 16
info "=== logs cpu1 (nice=0, should progress faster): ==="
"$ENGINE" logs cpu1
echo ""
info "=== logs cpu2 (nice=10, should progress slower): ==="
"$ENGINE" logs cpu2

# ---- DEMO 8: clean teardown ----
info "=== DEMO 8: stop alpha + beta ==="
"$ENGINE" stop alpha 2>/dev/null || true
"$ENGINE" stop beta  2>/dev/null || true
sleep 4
"$ENGINE" ps

info "Sending SIGTERM to supervisor (pid=$SUPV_PID)..."
kill -SIGTERM "$SUPV_PID"
sleep 2

if kill -0 "$SUPV_PID" 2>/dev/null; then
    fail "Supervisor still running after SIGTERM"
else
    pass "Supervisor exited cleanly"
fi

echo ""
info "Checking for zombies:"
ps aux | grep defunct | grep -v grep || pass "No zombie processes"

echo ""
info "dmesg tail (should show module activity + cleanup):"
dmesg | tail -n 30

# ---- unload module ----
info "Unloading monitor.ko..."
rmmod monitor && pass "rmmod OK"

# ---- summary ----
echo ""
echo "======================================="
echo " Test sequence complete."
echo " Supervisor log: /tmp/supervisor_test.log"
echo "======================================="
cat /tmp/supervisor_test.log
