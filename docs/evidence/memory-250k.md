# Host memory at 250,000 clients

**Hosts:** the three nodes on `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux
7.0.0-28; the clients on `gtr`, AMD Ryzen AI Max+ 395 (16 cores, 32 threads), 124 GiB RAM, NVMe SSD, Linux 6.17.0-29.
**Date:** 2026-09-15.

## The claim

Chart README, "Limits": "At 250,000 clients the host's available memory fell by about 11 GiB, from 78 GiB to 67 GiB
at its lowest sample, with the three nodes on it and the clients on another host (…, an earlier build), about 46 KiB a
client with its replicas, so a pod with the default 1 GiB holds about 22,700."

## What was run

- A load sweep of failovers: three nodes in one process on `ai2`, every client in a driver process on `gtr` over the
  network from twenty source addresses. Product timings.
- Three loads (100,000, 200,000 and 250,000 clients) by four runs by two victim policies (the node holding the most
  clients, and a follower): 24 runs, every one completed. The loads were interleaved within each policy; the two
  policies ran one after the other.
- A watcher on `ai2` recorded the host's available memory (`MemAvailable`, in whole GiB) about every 32 s through the
  batch: 167 samples.

## The build

The broker as of 2026-09-15, an earlier build than the chart's other figures. The broker has changed since, and this
load has not been measured again.

## Results

| Available memory on `ai2` | Samples |
|---|---|
| 78 GiB (before the batch, and the most common reading) | 98 of 167 |
| 79 GiB | 1 |
| 69–77 GiB | 61 |
| 67–68 GiB, the lowest | 7 (67 GiB in 3, each during a 250,000-client run) |

- The fall from 78 GiB to 67 GiB is about 11 GiB. Divided by 250,000 clients it is about 46 KiB a client (11 × 1,048,576
  KiB ÷ 250,000 = 46.1 KiB), each client's session with its replicas on the other nodes. The chart's figure for a pod
  of 1 GiB is 1,048,576 KiB ÷ 46.1 KiB, about 22,700 clients.

Every client came back in every run:

| Clients | Runs | Every client back after the crash |
|---|---|---|
| 100,000 | 8 | 29.8–53.0 s |
| 200,000 | 8 | 30.4–201.6 s |
| 250,000 | 8 | 31.2–140.5 s |

The batch's own reading is that the time tracked how many leases moved between the survivors rather than the client
count. Seven runs moved no lease (none lost and none adopted), 3 at 100,000 clients and 4 at 200,000, and finished in
29.8–36.7 s; no 250,000-client run moved none (the closest lost none and adopted one, and took 38.3 s). The run that
moved the most, 166,744 leases lost at 200,000 clients, was the slowest at 201.6 s; at 100,000 clients the slowest,
53.0 s, lost 24,278. The host's connection tracking peaked at about 568,000 entries.

## What this does not show

- The figure is the fall in the whole host's available memory, not a reading of the nodes' heap: it includes the
  kernel's memory for 250,000 connections and anything else the host did meanwhile.
- The readings are whole GiB, about 32 s apart, so the lowest point between samples may have been below 67 GiB.
- One batch on one host, with an earlier build. The per-client figure assumes the cost grows linearly with clients;
  the chart's per-pod capacity is arithmetic on it, not a measurement of a pod.
