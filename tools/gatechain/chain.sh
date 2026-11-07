#!/bin/bash
# chain.sh runs a chain of steps, one after another, while a work item holds
# a live claim on the host they run on, and can be stopped between them by a
# file.
#
#   chain.sh <host> <wi> <out-dir> <steps-file> <commit>
#   chain.sh --claim-held <wi> <rc>    (the claim decision alone; claims output on stdin)
#   chain.sh --tree-at <commit> <rc>   (the tree decision alone; the tree's state on stdin)
#
# Before each step the runner asks `reeve claims <host> --wi <wi>` itself
# (CHAIN_CLAIMS names another command of the same output). Without a live
# claim for the work item it stops there: done reads "no live claim for WI
# <wi> on <host> before <label>", it exits 3, and the step is not started.
#
# Then it reads the checkout the step runs in (`git rev-parse HEAD` and
# `git status --porcelain --untracked-files=no`, by the same local or ssh
# path as the step). Unless it is <commit>, the full 40-character hash, with
# no tracked change, it stops there: done reads "tree is not <commit> clean
# before <label>: <what it read>", it exits 4, and the step is not started.
# A step that changes a tracked file therefore ends the chain at the next.
#
# The steps file has one step per line, "<label> <command>"; blank lines and
# lines starting with # are skipped. A label is letters, digits, ".", "_" and
# "-", unique in the file; a "!" before it makes the step required, so the
# chain ends if it fails. Each command runs with bash -c in the directory the
# runner started in (or $CHAIN_DIR), its output in <out-dir>/<label>.log.
# With CHAIN_REMOTE set, each step runs on that ssh target instead, as a
# script on stdin from $CHAIN_DIR there (CHAIN_SSH names the ssh command);
# the out-dir stays here, beside the claims ledger.
#
# In <out-dir>:
#   pid      the runner's PID, its /proc start time and the steps file, written
#            at launch, so a PID can be checked before it is signalled
#   current  the running step's label and PID
#   summary  one line per step: "<label> exit=<n> secs=<n>"
#   stop     created by whoever wants the chain to end: it is checked before
#            each step, and the chain ends there
#   done     written last: "finished <n> steps", "stopped before <label>",
#            "ended at <label> (exit <n>)", or a refusal above
#
# The runner kills nothing and never removes the stop file.
set -u

# claim_held is the decision: whether `reeve claims <host> --wi <wi>`'s exit
# status rc and output (on stdin) show a live claim of exactly that work
# item. A lapsed claim, another work item's, or a failed command is none.
claim_held() {
	local wi=$1 rc=$2 line held=1
	while IFS= read -r line; do
		case $line in
		*" LAPSED") ;;
		*" WI $wi "*" until "*) held=0 ;;
		esac
	done
	[ "$rc" = 0 ] && [ "$held" = 0 ]
}

# tree_at is the decision: whether the state read from the checkout (on
# stdin: HEAD, then one line per tracked change), with the read's exit status
# rc, is exactly commit with nothing changed.
tree_at() {
	local want=$1 rc=$2 head line clean=0
	IFS= read -r head || head=
	while IFS= read -r line; do
		clean=1
	done
	[ "$rc" = 0 ] && [[ $want =~ ^[0-9a-f]{40}$ ]] && [ "$head" = "$want" ] && [ "$clean" = 0 ]
}

if [ "${1:-}" = --tree-at ]; then
	[ $# -eq 3 ] || { echo "usage: chain.sh --tree-at <commit> <rc>" >&2; exit 2; }
	tree_at "$2" "$3"
	exit
fi

if [ "${1:-}" = --claim-held ]; then
	[ $# -eq 3 ] || { echo "usage: chain.sh --claim-held <wi> <rc>" >&2; exit 2; }
	claim_held "$2" "$3"
	exit
fi

if [ $# -ne 5 ] || [ -z "$1" ] || [ -z "$2" ]; then
	echo "usage: chain.sh <host> <wi> <out-dir> <steps-file> <commit>" >&2
	exit 2
fi
host=$1 wi=$2 out=$3 steps=$4 commit=$5
if ! [[ $commit =~ ^[0-9a-f]{40}$ ]]; then
	echo "chain.sh: the commit must be a full 40-character hash, not '$commit'" >&2
	exit 2
fi
remote=${CHAIN_REMOTE:-}
dir=${CHAIN_DIR:-$PWD}
read -r -a claims <<< "${CHAIN_CLAIMS:-reeve claims}"
read -r -a sshcmd <<< "${CHAIN_SSH:-ssh -o BatchMode=yes}"

labels=() required=() commands=()
declare -A seen=()
n=0
while IFS= read -r line || [ -n "$line" ]; do
	n=$((n + 1))
	case $line in '' | '#'*) continue ;; esac
	label=${line%%[[:space:]]*}
	cmd=${line#"$label"}
	cmd=${cmd#"${cmd%%[![:space:]]*}"}
	req=0
	if [ "${label#!}" != "$label" ]; then
		req=1
		label=${label#!}
	fi
	if ! [[ $label =~ ^[A-Za-z0-9._-]+$ ]]; then
		echo "$steps:$n: the label must be letters, digits, '.', '_' or '-'" >&2
		exit 2
	fi
	if [ -z "$cmd" ]; then
		echo "$steps:$n: step $label has no command" >&2
		exit 2
	fi
	if [ -n "${seen[$label]:-}" ]; then
		echo "$steps:$n: step $label is also on line ${seen[$label]}" >&2
		exit 2
	fi
	seen[$label]=$n
	labels+=("$label")
	required+=("$req")
	commands+=("$cmd")
done < "$steps"
if [ ${#labels[@]} -eq 0 ]; then
	echo "$steps: no steps" >&2
	exit 2
fi

mkdir -p "$out" || exit 2
rm -f "$out/done" "$out/summary" "$out/current"
start=$(awk '{print $22}' "/proc/$$/stat")
echo "$$ $start $steps" > "$out/pid"

finish() {
	echo "$1" > "$out/done"
	rm -f "$out/current"
	exit 0
}

for i in "${!labels[@]}"; do
	label=${labels[$i]}
	if [ -e "$out/stop" ]; then
		echo "stopped before $label" >> "$out/summary"
		finish "stopped before $label"
	fi
	answer=$("${claims[@]}" "$host" --wi "$wi" 2>&1)
	rc=$?
	if ! claim_held "$wi" "$rc" <<< "$answer"; then
		echo "no live claim for WI $wi on $host before $label" >> "$out/summary"
		echo "no live claim for WI $wi on $host before $label" > "$out/done"
		rm -f "$out/current"
		exit 3
	fi
	look='git rev-parse HEAD && git status --porcelain --untracked-files=no'
	if [ -n "$remote" ]; then
		state=$(printf 'cd %q || exit 2\n%s\n' "$dir" "$look" | "${sshcmd[@]}" "$remote" 'bash -s' 2>&1)
	else
		state=$(cd "$dir" 2>&1 && bash -c "$look" 2>&1)
	fi
	rc=$?
	if ! tree_at "$commit" "$rc" <<< "$state"; then
		why="tree is not $commit clean before $label: $(tr '\n' ' ' <<< "$state" | cut -c1-300)"
		echo "$why" >> "$out/summary"
		echo "$why" > "$out/done"
		rm -f "$out/current"
		exit 4
	fi
	t0=$(date +%s)
	if [ -n "$remote" ]; then
		printf 'cd %q || exit 2\n%s\n' "$dir" "${commands[$i]}" | "${sshcmd[@]}" "$remote" 'bash -s' > "$out/$label.log" 2>&1 &
	else
		(cd "$dir" && exec bash -c "${commands[$i]}") > "$out/$label.log" 2>&1 < /dev/null &
	fi
	pid=$!
	echo "$label $pid" > "$out/current"
	wait "$pid"
	rc=$?
	echo "$label exit=$rc secs=$(($(date +%s) - t0))" >> "$out/summary"
	if [ "$rc" -ne 0 ] && [ "${required[$i]}" -eq 1 ]; then
		finish "ended at $label (exit $rc)"
	fi
done
finish "finished ${#labels[@]} steps"
