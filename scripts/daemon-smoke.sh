#!/bin/sh
set -eu

scope=user
service_user=
binary=${MUTSUKI_SERVICE_BIN:-"$(pwd)/target/debug/mutsuki-service"}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --scope)
      scope=$2
      shift 2
      ;;
    --service-user)
      service_user=$2
      shift 2
      ;;
    --binary)
      binary=$2
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [ "$scope" != user ] && [ "$scope" != system ]; then
  echo "--scope must be user or system" >&2
  exit 2
fi
if [ "$scope" = system ] && [ -z "$service_user" ]; then
  echo "system scope requires --service-user" >&2
  exit 2
fi
if [ ! -x "$binary" ]; then
  echo "service binary is not executable: $binary" >&2
  exit 2
fi

root=$(mktemp -d /tmp/mutsuki-daemon-smoke.XXXXXX)
instance="daemon-smoke-$$"
config="$root/service.toml"
socket="$root/home/run/$instance.sock"
installed=false

mkdir -p "$root/home/data" "$root/home/logs" "$root/home/plugins" "$root/home/run"
chmod 755 "$root" "$root/home" "$root/home/data" "$root/home/logs" "$root/home/plugins" "$root/home/run"

cleanup() {
  if [ "$installed" = true ]; then
    if [ "$scope" = system ]; then
      sudo "$binary" --config "$config" uninstall --scope system >/dev/null 2>&1 || true
    else
      "$binary" --config "$config" uninstall --scope user >/dev/null 2>&1 || true
    fi
  fi
  if [ "${MUTSUKI_SMOKE_KEEP_ROOT:-0}" = 1 ]; then
    echo "preserved smoke root: $root" >&2
  else
    rm -rf "$root"
  fi
}
trap cleanup EXIT INT TERM

cat >"$config" <<EOF
[service]
profile = "daemon-smoke"
instance_id = "$instance"
home_dir = "$root/home"
data_dir = "data"
log_dir = "logs"
plugin_dir = "plugins"
run_dir = "run"

[ipc]
enabled = true
transport = "unix-socket"
name = "$instance"
token = "daemon-smoke-token"

[plugins]
dynamic_dirs = []
disabled_dir = "plugins/disabled"

[observe]
console = false
json = false
log_file = "service.log"
panic_file = "panic.log"
EOF

if [ "$scope" = system ]; then
  sudo "$binary" --config "$config" install --scope system --service-user "$service_user"
  installed=true
  if ! sudo "$binary" --config "$config" start --scope system; then
    unit="mutsuki-service-$instance.service"
    sudo systemctl status "$unit" --no-pager >&2 || true
    sudo systemctl cat "$unit" >&2 || true
    sudo systemd-analyze verify "/etc/systemd/system/$unit" >&2 || true
    exit 1
  fi
else
  "$binary" --config "$config" install --scope user
  installed=true
  "$binary" --config "$config" start --scope user
fi

healthy=false
attempt=0
while [ "$attempt" -lt 300 ]; do
  if "$binary" --config "$config" health >"$root/health.json" 2>/dev/null; then
    healthy=true
    break
  fi
  attempt=$((attempt + 1))
  sleep 0.1
done
if [ "$healthy" != true ]; then
  echo "service did not become healthy" >&2
  if [ "$(uname -s)" = Darwin ]; then
    launchctl print "gui/$(id -u)/io.github.sena-nana.mutsuki-service.$instance" >&2 || true
  fi
  if [ -f "$root/home/logs/service.log" ]; then
    tail -n 40 "$root/home/logs/service.log" >&2
  fi
  if [ -f "$root/home/logs/panic.log" ]; then
    tail -n 40 "$root/home/logs/panic.log" >&2
  fi
  exit 1
fi
grep '"service": "ok"' "$root/health.json" >/dev/null

"$binary" --config "$config" stop >"$root/stop.json"

attempt=0
while [ -e "$socket" ] && [ "$attempt" -lt 300 ]; do
  attempt=$((attempt + 1))
  sleep 0.1
done
if [ -e "$socket" ]; then
  echo "Unix socket was not removed after graceful shutdown: $socket" >&2
  exit 1
fi

if [ "$scope" = system ]; then
  sudo "$binary" --config "$config" uninstall --scope system
else
  "$binary" --config "$config" uninstall --scope user
fi
installed=false

echo "daemon smoke passed: $(uname -s) scope=$scope"
