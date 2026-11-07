#!/bin/bash
# sync.sh carries branch wi-<wi> to a test host as a bundle over <base> and
# checks out its tip in ~/hoardmq-gates-<wi>, cloned from an existing gate
# checkout the first time, but only while the work item holds a live claim on
# the host: without one it moves nothing and exits 3.
#
#   sync.sh <host> <wi> <base> [clone-from]
#
# It prints the commit checked out on the host and the count of dirty files.
# clone-from defaults to ~/hoardmq-gates-2458. Run from a checkout of the
# repository holding wi-<wi>. CHAIN_CLAIMS, CHAIN_SSH and CHAIN_SCP name other
# commands of the same output, for tests.
set -u
if [ $# -lt 3 ] || [ $# -gt 4 ]; then
	echo "usage: sync.sh <host> <wi> <base> [clone-from]" >&2
	exit 2
fi
host=$1 wi=$2 base=$3 from=${4:-\~/hoardmq-gates-2458}
here=$(cd "$(dirname "$0")" && pwd)
read -r -a claims <<< "${CHAIN_CLAIMS:-reeve claims}"
read -r -a sshcmd <<< "${CHAIN_SSH:-ssh -o BatchMode=yes}"
read -r -a scpcmd <<< "${CHAIN_SCP:-scp -q -o BatchMode=yes}"

# The same decision the runner makes before every step.
answer=$("${claims[@]}" "$host" --wi "$wi" 2>&1)
rc=$?
if ! bash "$here/chain.sh" --claim-held "$wi" "$rc" <<< "$answer"; then
	echo "no live claim for WI $wi on $host: nothing synced" >&2
	exit 3
fi

set -e
repo=$(git rev-parse --show-toplevel)
want=$(git -C "$repo" rev-parse "wi-$wi")
mkdir -p "$repo/tmp/$wi"
b=$repo/tmp/$wi/wi$wi.bundle
git -C "$repo" bundle create "$b" "$base..wi-$wi" > /dev/null 2>&1
"${scpcmd[@]}" "$b" "$host:hoardmq-wi$wi.bundle"
"${sshcmd[@]}" "$host" 'bash -s' << SH
set -e
if [ ! -d ~/hoardmq-gates-$wi ]; then git clone -q $from ~/hoardmq-gates-$wi </dev/null; fi
cd ~/hoardmq-gates-$wi
git cat-file -e $base^{commit}
git fetch -q ~/hoardmq-wi$wi.bundle "+refs/heads/wi-$wi:refs/heads/wi-$wi" </dev/null
git checkout -q --detach $want </dev/null
git log -1 --format='%H %s'
git status --porcelain | wc -l
SH
