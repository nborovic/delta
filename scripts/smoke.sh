#!/usr/bin/env bash
# Smoke test for a running delta proxy in front of three Surfpool nodes, set
# up like config.surfpool.toml (or a project scaffolded from it):
#
#   surfpool start --offline --no-studio --no-tui -y -p 8899 -w 8900
#   surfpool start --offline --no-studio --no-tui -y -p 8999 -w 9000
#   surfpool start --offline --no-studio --no-tui -y -p 9099 -w 9100
#   <run the proxy>
#   scripts/smoke.sh
#
# Settings (environment variables, defaults in brackets):
#   PROXY              proxy base URL                  [http://127.0.0.1:8080]
#   KEY                API key for every route         [the config.surfpool.toml dev key]
#   LIMITED_KEY        key for the cache route only, 5 rps   [its dev key]
#   RPC_A, RPC_B, RPC_C  the three Surfpool nodes, called directly to set up and
#                      check state                     [http://127.0.0.1:8899, :8999, :9099]
#
# Needs curl, jq, python3 and the Solana CLI (solana, solana-keygen), which is
# the real client: wallet commands over HTTP, `solana logs` over WebSocket.
# Exits non-zero if any check fails.

set -uo pipefail

PROXY=${PROXY:-http://127.0.0.1:8080}
KEY=${KEY:-11111111-2222-4333-8444-555555555555}
LIMITED_KEY=${LIMITED_KEY:-22222222-3333-4444-8555-666666666666}
RPC_A=${RPC_A:-http://127.0.0.1:8899}
RPC_B=${RPC_B:-http://127.0.0.1:8999}
RPC_C=${RPC_C:-http://127.0.0.1:9099}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

passed=0
failed=0
ok() { passed=$((passed + 1)); printf '  \033[32mok\033[0m    %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf '  \033[31mFAIL\033[0m  %s\n        got: %s\n' "$1" "${2:0:300}"; }
section() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# rpc URL METHOD [PARAMS_JSON]: prints the response body.
rpc() {
  local body
  if [ -n "${3:-}" ]; then
    body=$(jq -nc --arg m "$2" --argjson p "$3" '{jsonrpc: "2.0", id: 1, method: $m, params: $p}')
  else
    body=$(jq -nc --arg m "$2" '{jsonrpc: "2.0", id: 1, method: $m}')
  fi
  curl -s -X POST -H 'content-type: application/json' -d "$body" "$1"
}

# route NAME [KEY]: the URL of a route.
route() { echo "$PROXY/rpc/$1?api-key=${2:-$KEY}"; }

# http_status URL: the HTTP status of a getLatestBlockhash request.
http_status() {
  curl -s -o /dev/null -w '%{http_code}' -X POST -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash"}' "$1"
}

SYSTEM_PROGRAM=11111111111111111111111111111111

# expect NAME JSON JQ_FILTER: passes when the filter is true.
expect() {
  if printf '%s' "$2" | jq -e "$3" >/dev/null 2>&1; then ok "$1"; else bad "$1" "$2"; fi
}

# expect_eq NAME GOT WANTED
expect_eq() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "$2 (wanted $3)"; fi; }

# A random account address (base58 of 32 random bytes).
pubkey() {
  python3 - <<'PY'
import os
alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
raw = os.urandom(32)
n, out = int.from_bytes(raw, "big"), ""
while n:
    n, r = divmod(n, 58)
    out = alphabet[r] + out
print("1" * (len(raw) - len(raw.lstrip(b"\0"))) + out)
PY
}

balance() { rpc "$1" getBalance "[\"$2\", {\"commitment\": \"processed\"}]" | jq -r '.result.value // empty'; }

# airdrop URL ADDRESS LAMPORTS: airdrops straight on one node and waits for it.
airdrop() {
  rpc "$1" requestAirdrop "[\"$2\", $3]" >/dev/null
  for _ in $(seq 1 100); do
    [ "$(balance "$1" "$2")" = "$3" ] && return 0
    sleep 0.2
  done
  bad "airdrop on $1 landed" "balance $(balance "$1" "$2")"
}

# wait_balance URL ADDRESS LAMPORTS: waits up to 20 s for that balance.
wait_balance() {
  for _ in $(seq 1 100); do
    [ "$(balance "$1" "$2")" = "$3" ] && return 0
    sleep 0.2
  done
  return 1
}

# ---------------------------------------------------------------- checks

section "Setup"
if [ "$(curl -s "$PROXY/health?api-key=$KEY")" = "ok" ]; then ok "proxy answers /health at $PROXY"; else
  bad "proxy answers /health at $PROXY" "no answer: is it running, and is KEY right?"
  exit 1
fi
for node in "$RPC_A" "$RPC_B" "$RPC_C"; do
  expect "Surfpool node at $node answers" "$(rpc "$node" getHealth)" '.result == "ok"'
done

section "API keys"
expect_eq "no key: HTTP 401" "$(http_status "$PROXY/rpc/hedge")" 401
expect "wrong key: -33091 unauthorized" "$(rpc "$(route hedge 00000000-0000-4000-8000-000000000000)" getLatestBlockhash)" '.error.code == -33091'
expect_eq "/health without a key: HTTP 401" "$(curl -s -o /dev/null -w '%{http_code}' "$PROXY/health")" 401
expect_eq "limited key: refused on a route it does not list" "$(http_status "$(route hedge "$LIMITED_KEY")")" 401
codes=""
for _ in 1 2 3 4 5 6 7; do
  codes="$codes $(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash"}' "$(route cache "$LIMITED_KEY")")"
done
if [[ "$codes" == *200* && "$codes" == *429* ]]; then ok "limited key: rate limited at 5 rps (HTTP:$codes)"; else
  bad "limited key: rate limited at 5 rps" "HTTP:$codes"
fi

section "Routing"
expect "hedge: getAccountInfo of the System Program" \
  "$(rpc "$(route hedge)" getAccountInfo "[\"$SYSTEM_PROGRAM\", {\"encoding\": \"base64\"}]")" '.result.value.executable == true'
expect "hedge: getMultipleAccounts, one known and one empty" \
  "$(rpc "$(route hedge)" getMultipleAccounts "[[\"$SYSTEM_PROGRAM\", \"$(pubkey)\"], {\"encoding\": \"base64\"}]")" '.result.value[0].executable == true and .result.value[1] == null'
expect "unknown route: -33090" "$(rpc "$(route nope)" getLatestBlockhash)" '.error.code == -33090'
expect "safe: a method it does not list is refused (-32601)" "$(rpc "$(route safe)" getEpochInfo)" '.error.code == -32601'
expect "Surfpool's own errors pass through (-32602)" "$(rpc "$(route hedge)" getBalance '[]')" '.error.code == -32602'
batch=$(curl -s -X POST -H 'content-type: application/json' "$(route hedge)" -d \
  '[{"jsonrpc":"2.0","id":"a","method":"getLatestBlockhash"},{"jsonrpc":"2.0","id":2,"method":"getEpochInfo"},{"jsonrpc":"2.0","id":3,"method":"noSuchMethod"}]')
expect "batch: answers in order, ids kept" "$batch" \
  'length == 3 and .[0].id == "a" and (.[0].result.value.blockhash | type == "string") and .[1].id == 2 and (.[1].result.absoluteSlot | type == "number") and .[2].id == 3 and .[2].error.code == -32601'
expect "empty batch: -32600" "$(curl -s -X POST -H 'content-type: application/json' -d '[]' "$(route hedge)")" '.error.code == -32600'
expect_eq "a notification (no id): HTTP 200 and no body, as on a node" \
  "$(curl -s -w '%{http_code}' -X POST -H 'content-type: application/json' -d '{"jsonrpc":"2.0","method":"getSlot"}' "$(route hedge)")" 200

section "Hedge (surf-a and surf-b raced)"
expect "getSlot is answered" "$(rpc "$(route hedge)" getSlot)" '.result | type == "number"'
writer=$(pubkey)
expect "requestAirdrop is accepted" "$(rpc "$(route hedge)" requestAirdrop "[\"$writer\", 1000000000]")" '.result | type == "string"'
for node in "$RPC_A" "$RPC_B"; do
  if wait_balance "$node" "$writer" 1000000000; then ok "the write reached $node"; else
    bad "the write reached $node" "balance $(balance "$node" "$writer")"
  fi
done
sleep 1
expect_eq "the write never reached $RPC_C (not on this route)" "$(balance "$RPC_C" "$writer")" 0

section "Quorum (all three asked, two must agree)"
acct=$(pubkey)
params="[\"$acct\", {\"commitment\": \"processed\"}]"
airdrop "$RPC_A" "$acct" 500000000
expect "surf-a alone is outvoted: surf-b and surf-c agree on 0" "$(rpc "$(route safe)" getBalance "$params")" '.result.value == 0'
airdrop "$RPC_B" "$acct" 500000000
expect "surf-a and surf-b agree: their answer wins" "$(rpc "$(route safe)" getBalance "$params")" '.result.value == 500000000'
split=$(pubkey)
split_params="[\"$split\", {\"commitment\": \"processed\"}]"
airdrop "$RPC_A" "$split" 100000000
airdrop "$RPC_B" "$split" 200000000
airdrop "$RPC_C" "$split" 300000000
expect "three different answers: refused (-33094)" "$(rpc "$(route safe)" getBalance "$split_params")" '.error.code == -33094'
expect "getAccountInfo of the System Program: all three agree" \
  "$(rpc "$(route safe)" getAccountInfo "[\"$SYSTEM_PROGRAM\", {\"encoding\": \"base64\"}]")" '.result.value.executable == true'

section "Cache (getLatestBlockhash only, 5 s, weighted 3:2:1)"
expect "getSlot is refused here (-32601)" "$(rpc "$(route cache)" getSlot)" '.error.code == -32601'
# Uncached, the answer changes every slot (400 ms). Over 2 s of requests a
# 5 s cache leaves one answer, whichever node it came from (two if the entry
# an earlier check stored expires during the run).
answers=""
for _ in $(seq 1 20); do
  answers="$answers$(rpc "$(route cache)" getLatestBlockhash | jq -c '.result // empty')"$'\n'
  sleep 0.1
done
answered=$(printf '%s' "$answers" | grep -c .)
distinct=$(printf '%s' "$answers" | sort -u | grep -c .)
if [ "$answered" -eq 20 ] && [ "$distinct" -le 2 ]; then
  ok "20 requests over 2 s got $distinct distinct answers: served from the cache (one entry, not one per node)"
else
  bad "20 requests over 2 s, at most 2 distinct answers" "$answered answered, $distinct distinct"
fi

section "Failover (priority surf-a, surf-b, surf-c)"
marker=$(pubkey)
airdrop "$RPC_A" "$marker" 7000000
expect "surf-a answers while it is healthy (only it has this balance)" \
  "$(rpc "$(route failover)" getBalance "[\"$marker\", {\"commitment\": \"processed\"}]")" '.result.value == 7000000'

section "Solana CLI (failover: one node, every method; logs over stream on port + 1)"
cli_url=$(route failover)
solana-keygen new --no-bip39-passphrase -s --force -o "$TMP/payer.json" >/dev/null
payer=$(solana-keygen pubkey "$TMP/payer.json")
recipient=$(pubkey)
cli() { solana -u "$cli_url" -k "$TMP/payer.json" --commitment confirmed "$@" 2>&1; }
out=$(cli airdrop 2 "$payer")
if [[ "$out" == *"2 SOL"* ]]; then ok "solana airdrop 2: confirmed"; else bad "solana airdrop 2: confirmed" "$out"; fi
expect_eq "solana balance: 2 SOL" "$(cli balance "$payer")" "2 SOL"
# `solana logs` subscribes through /rpc/stream (all three nodes) before the
# transfer is sent. No --ws: the CLI looks for the WebSocket on the port + 1,
# and delta serves there too.
solana logs "$recipient" -u "$(route stream)" --commitment confirmed >"$TMP/logs.out" 2>&1 &
logs_pid=$!
for _ in $(seq 1 50); do grep -q Streaming "$TMP/logs.out" && break; sleep 0.1; done
sleep 0.5
out=$(cli transfer "$recipient" 0.5 --allow-unfunded-recipient --output json)
sig=$(printf '%s' "$out" | jq -r '.signature // empty' 2>/dev/null)
if [ -n "$sig" ]; then ok "solana transfer 0.5: sent and confirmed"; else bad "solana transfer 0.5: sent and confirmed" "$out"; fi
out=$(cli confirm "$sig")
if [[ "$out" == Confirmed* || "$out" == Finalized* ]]; then ok "solana confirm: $out"; else bad "solana confirm" "$out"; fi
expect_eq "solana balance of the recipient: 0.5 SOL" "$(cli balance "$recipient")" "0.5 SOL"
expect_eq "the transfer landed on surf-a" "$(balance "$RPC_A" "$recipient")" 500000000
for _ in $(seq 1 100); do [ -n "$sig" ] && grep -q "$sig" "$TMP/logs.out" && break; sleep 0.1; done
kill "$logs_pid" 2>/dev/null
if [ -n "$sig" ] && grep -q "Signature: $sig" "$TMP/logs.out" && grep -q "Status: Ok" "$TMP/logs.out"; then
  ok "solana logs: the transfer's logs arrived over the WebSocket (stream route)"
else
  bad "solana logs: the transfer's logs arrived over the WebSocket" "$(cat "$TMP/logs.out")"
fi

printf '\n%d passed, %d failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
