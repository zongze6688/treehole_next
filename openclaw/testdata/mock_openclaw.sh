#!/usr/bin/env bash
# mock_openclaw.sh — a fake `openclaw fleet` CLI for integration tests.
#
# Emulates the documented openclaw fleet subcommands against mock data. It is
# driven like: bash mock_openclaw.sh fleet <subcommand> <tenant> [--json ...args]
#   $1 is the CLI subcommand ("fleet"), $2 the fleet subcommand, $3 the tenant.
#
# Every invocation is appended to the file named by MOCK_FLEET_LOG (default
# /tmp/mock_openclaw.log) as a single "echo $*" line, so tests can assert the
# exact argument list the control plane produced.
set -euo pipefail

log="${MOCK_FLEET_LOG:-/tmp/mock_openclaw.log}"
echo "$*" >> "$log"

sub="${2:-}"
tenant="${3:-}"

case "$sub" in
  create)
    # shellcheck disable=SC2059
    printf '{"tenant":"%s","containerName":"openclaw-cell-%s","port":19100,"image":"ghcr.io/openclaw/openclaw:latest","runtime":"docker","started":true,"token":"mock-token","tokenNote":"shown once","url":"http://127.0.0.1:19100","nextStep":"configure"}\n' "$tenant" "$tenant"
    ;;
  status)
    # shellcheck disable=SC2059
    printf '{"tenant":"%s","containerName":"openclaw-cell-%s","runtime":"docker","port":19100,"image":"ghcr.io/openclaw/openclaw:latest","created":"2026-01-01T00:00:00Z","dataDir":"/tmp/fleet/cells/%s","container":{"state":"running","running":true,"managed":true},"health":{"status":"ok","url":"http://127.0.0.1:19100/healthz","httpStatus":200}}\n' "$tenant" "$tenant" "$tenant"
    ;;
  start|stop|restart)
    # shellcheck disable=SC2059
    printf '{"tenant":"%s","action":"%s"}\n' "$tenant" "$sub"
    ;;
  rm)
    # shellcheck disable=SC2059
    printf '{"tenant":"%s","action":"rm","dataPurged":true}\n' "$tenant"
    ;;
  logs)
    echo "mock log line"
    ;;
  *)
    echo "mock_openclaw: unknown fleet subcommand: $sub" >&2
    exit 1
    ;;
esac
exit 0
