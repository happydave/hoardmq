#!/usr/bin/env bash
# Captures the fixtures beside this file from a real host, with the commands
# sim/scale.go's factsScript and samplerScript run (kept the same by hand). The
# committed captures have their host names and addresses replaced with
# documentation ones (RFC 5737).
#   facts:   ssh <host> 'bash -s facts <binary>' < capture.sh > facts-<host>.txt
#   sampler: ssh <host> 'bash -s sampler <rundir>' < capture.sh > sampler-<host>.txt (each <rundir>/<node>/pid names a process)
mode=$1; shift
if [ "$mode" = facts ]; then
echo "hostname=$(hostname)"; echo "kernel=$(uname -r)"; echo "cpus=$(nproc)"
awk '/^MemAvailable:/{print "mem_available_kb="$2}' /proc/meminfo
echo "nofile_soft=$(ulimit -Sn)"; echo "nofile_hard=$(ulimit -Hn)"
for k in net/netfilter/nf_conntrack_max net/netfilter/nf_conntrack_count net/core/somaxconn net/ipv4/tcp_max_syn_backlog net/core/netdev_max_backlog net/ipv4/ip_local_port_range; do
  if [ -r /proc/sys/$k ]; then echo "sysctl.$k=$(tr '\t' ' ' < /proc/sys/$k)"; fi
done
echo "load=$(cut -d' ' -f1 /proc/loadavg)"
echo "ntp=$(timedatectl show -p NTPSynchronized --value 2>/dev/null)"
echo "addrs=$(ip -4 -o addr show | awk '{split($4,a,"/"); printf "%s ", a[1]}')"
echo "listen=$(ss -Htln | awk '{printf "%s ", $4}')"
b=$1
if [ -r "$b" ]; then echo "sha256=$b $(sha256sum "$b" | cut -d' ' -f1)"; else echo "sha256=$b missing"; fi
echo rundir=absent
else
set -- "$1"
for _ in 1 2 3; do
  l="S $(date +%s%3N) mem=$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)"
  l="$l net=$(awk 'NR>2{gsub(":"," "); if($1=="lo"){lr+=$2; lt+=$10} else {nr+=$2; nt+=$10}} END{printf "%d,%d,%d,%d", lr,lt,nr,nt}' /proc/net/dev)"
  l="$l tcp=$(awk '/^TCP:/{print $3","$7}' /proc/net/sockstat)"
  l="$l ct=$(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || echo -)"
  for f in "$1"/*/pid; do
    [ -r "$f" ] || continue
    n=$(basename "$(dirname "$f")"); p=$(cat "$f")
    r=$(awk '/^VmRSS:/{print $2}' /proc/$p/status 2>/dev/null)
    t=$(awk '{s=$0; sub(/.*\) /,"",s); split(s,a," "); print a[12]+a[13]}' /proc/$p/stat 2>/dev/null)
    l="$l $n=${r:--},${t:--}"
  done
  echo "$l"
  sleep 0.5
done
fi
