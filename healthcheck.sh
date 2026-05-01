#!/bin/bash
# Hourly health check + self-heal for backlink-finder.fly.dev
# Installed as: crontab entry running every hour

LOG="/tmp/backlink-api/healthcheck.log"
APP="backlink-finder"
MACHINE="0802036b244d08"
FLYCTL="$HOME/.fly/bin/flyctl"
export FLY_API_TOKEN='FlyV1 fm2_lJPECAAAAAAAE2icxBAjExloZlkFIW8mUYvehAj8wrVodHRwczovL2FwaS5mbHkuaW8vdjGWAJLOABhi0x8Lk7lodHRwczovL2FwaS5mbHkuaW8vYWFhL3YxxDzDjTtnGxDkpNS5hxAA/4e3c84uT0pPRGpOdYbh9Lu9wRrjblc9lxLXKPijFPfwU9Xan4jABivvWNmgD+LEThHgAQZFSSLQhCrG6D+S3nKD9EZgvGwHrJFB3/FRu2o64JgWObb63szczPzP5UAKYei5JhVVdGFLTZANLa9/ay/8cC5tMrz7X5464anPEg2SlAORgc4BC71DHwWRgqdidWlsZGVyH6J3Zx8BxCA/Nc30K7xbOYuwaVJDjh5NtcKr4NHJY3BeSEW/4oSYZQ==,fm2_lJPEThHgAQZFSSLQhCrG6D+S3nKD9EZgvGwHrJFB3/FRu2o64JgWObb63szczPzP5UAKYei5JhVVdGFLTZANLa9/ay/8cC5tMrz7X5464anPEsQQYPagfcHBzIJuIRNR2Iu+gsO5aHR0cHM6Ly9hcGkuZmx5LmlvL2FhYS92MZgEks5p88F7zwAAAAEl69+ZF84AF1+QCpHOABdfkAzEEI7J0tP7vEuqCiGivQlGdQvEIOWTdbcEtdE7+8tae4MY37B5nNJ0Le/pQcrqyUln7dts'

TS=$(date '+%Y-%m-%d %H:%M:%S')

# Check health endpoint (timeout 10s)
STATUS=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "https://backlink-finder.fly.dev/health" 2>/dev/null)

if [ "$STATUS" = "200" ]; then
    echo "$TS OK (HTTP $STATUS)" >> "$LOG"
else
    echo "$TS FAIL (HTTP $STATUS) — restarting machine" >> "$LOG"
    
    # Restart the machine
    "$FLYCTL" machines restart "$MACHINE" -a "$APP" >> "$LOG" 2>&1
    
    # Wait 30s, re-check
    sleep 30
    STATUS2=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "https://backlink-finder.fly.dev/health" 2>/dev/null)
    echo "$TS AFTER-RESTART (HTTP $STATUS2)" >> "$LOG"
fi

# Keep log file trimmed to last 200 lines
tail -200 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
