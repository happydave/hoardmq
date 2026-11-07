# How long a client-list page holds the lease table's read lock

**Host:** `ai`, AMD Ryzen 9 7950X3D (16 cores, 32 threads), 93 GiB RAM, NVMe SSD, Linux 6.8.0-146.
**Date:** 2026-10-04.

## The claim

docs/operations.md, ["Admin API"](../operations.md#admin-api): "One search at a time walks the table, holding its read lock only while it keeps one page:
1.4–2.3 ms on average (2.7 ms at most) at 100,000 clients in a probe of the design, not a measurement of the shipped
code".

## What was run

- A probe test in the lease package, written to settle the design before the code and not kept in the repository. It
  filled a lease table and timed how long a search held the table's read lock, by what it collected under the lock:
  every match copied (the shape the table offered before), or one page of 100 or 1,000 kept by a bounded heap.
- 20 scans a case, in one process, the host otherwise quiet (load 0.3), the probe niced.
- A writer applying one-operation commands ran beside the page scans, standing in for the consensus loop's apply,
  and its waits were timed.

## The build

The broker as of 2026-10-03 with the probe added. The shipped search (2026-10-04) keeps one page under the read lock
by a bounded heap, as the probe did; its page bound is tested by `TestScanKeepsOnlyAPage` in
`internal/cluster/lease`, which checks what a scan keeps, not how long it holds the lock.

## Results

| Leases | Collected under the read lock | Hold, mean / max |
|---|---|---|
| 100,000 | every match, copied | 15.5 / 26.1 ms |
| 100,000 | one page of 100 or 1,000, by a bounded heap | 1.4–2.3 / 2.1–2.7 ms |
| 100,000 | a substring matching 100 | 1.2 / 1.5 ms |
| 1,000,000 | every match | 160 / 267 ms |
| 1,000,000 | one page of 100 or 1,000 | 41–42 / 52–64 ms |

The writer beside the page scans waited at most 2.9 ms at 100,000 leases and 48.6 ms at 1,000,000, with a median
under 1.1 µs at both.

## What this does not show

- It is a probe of the design, not the shipped code: the shipped search's hold was not timed.
- Not measured: a search during a real takeover storm. The probe's writer stands in for the storm's applies.
- One host, 20 scans a case.
