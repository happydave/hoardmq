# What a client that stops acknowledging holds

**Host:** `ai2`, AMD Ryzen 9 9950X3D (16 cores, 32 threads), 91 GiB RAM, NVMe SSD, Linux 7.0.0-28, the whole host
reserved.
**Date:** 2026-11-03.

## The claims

- docs/clients-and-sessions.md, ["Connection behaviour"](../clients-and-sessions.md#connection-behaviour): "at 256-byte payloads a client that stops holds about 1.52 MB instead of about 52.5
  MB, and a client acknowledging 1 ms after each message still received 161–172 thousand messages a second, against
  128–198 thousand without the bound, where the test's sender was the limit (measured on loopback …)".
- Chart README, "Limits": "measured on one broker over loopback with 256-byte payloads, a client that stops
  acknowledging holds about 52.5 MB with its 1,000 queued, against about 1.52 MB at 1,024 (the broker's
  `TestOutboundInflightScale`, three runs each …). At 1,024 a client acknowledging at once received 178–186 thousand
  messages a second, against 141–196 thousand without the bound, and one acknowledging 1 ms after each message
  161–172 thousand, against 128–198 thousand; without the bound the test's sender was the limit."

## What was run

- `TestOutboundInflightScale` in `internal/broker`, run with `HOARDMQ_INFLIGHT_SCALE` set: one broker, clients over
  loopback, 256-byte payloads.
- A slow client stops acknowledging; the test fills its in-flight quota and 1,000 queued messages, asserts both
  counts, and reads the heap it holds.
- A fast client receives 100,000 messages, acknowledging each at once or 1 ms after it; the test asserts all 100,000
  arrived and reads the delivery rate.
- 3 runs at each bound. Before the cap existed, a bound was set by the subscriber's Receive Maximum, which gives the
  broker the same send quota; a 3.1.1 subscriber sends none, so its bound was 65,535.

## The builds

- **Before:** `main` of 2026-11-03 with the measurement test added.
- **The cap:** the build of 2026-11-03 that adds `limits.maxInflightMessages`, the in-flight cap the chart sets to
  1,024; measured at a cap of 1,024.

## Results

| Bound | Heap a slow client holds (in flight + 1,000 queued) | Fast client, acknowledging at once | Fast client, 1 ms after each |
|---|---|---|---|
| 3.1.1 client (65,535), before | 52.5, 52.7, 52.5 MB | 141, 196, 181 thousand a second* | 184, 163, 198 thousand a second* |
| Receive Maximum 65,535, before | 52.6, 52.5, 52.4 MB | 195, 184, 190 thousand* | 156, 144, 128 thousand* |
| Receive Maximum 1,024, before | 1.50, 1.50, 1.50 MB | 186, 185, 182 thousand | 161, 161, 164 thousand |
| Receive Maximum 256, before | 0.90, 0.90, 0.91 MB | 210, 200, 200 thousand | 150, 150, 154 thousand |
| Receive Maximum 32, before | 0.73, 0.73, 0.73 MB | 203, 74, 207 thousand | 6.7, 7.2, 7.1 thousand |
| 3.1.1 client under the cap of 1,024 | 1.517, 1.519, 1.518 MB | 178, 179, 179 thousand | 172, 171, 164 thousand |

\* At 65,535 the injection took as long as the delivery, so these figures are the test's injection rate, not the
client's. At 1,024 and below delivery outlasted injection by 0.2–0.3 s, so those figures are the delivery's.

- At 65,535 a slow client holds about 790 bytes a message.
- With the cap, a 5.0 client whose Receive Maximum is 256 keeps its 256 (0.91 MB).

## What this does not show

- Loopback and one broker: a real network's round trip makes the bound matter at higher values. A bound of B over a
  round trip of R allows at most B/R messages a second to one client.
- Whether the bound costs a fast client any rate: at 1,024 the rates (178–186 thousand a second at once, 161–172
  thousand at 1 ms) fall inside the ranges without the bound (141–196 and 128–198 thousand), but those ranges are the
  test's sending rate, not the client's, so the client's own ceiling without the bound was not measured. Three runs
  each.
- At 1,024 a client acknowledging 1 ms after each message ran slower than one acknowledging at once: 161–172 thousand
  a second against 178–186 thousand (164–172 against 178–179 on the cap's build).
- One run of three at a Receive Maximum of 256 with a 1 ms acknowledgement, on the cap's build, read 61 thousand a
  second against 146 and 148 thousand; it is not attributed.
- One payload size and one client of each kind.
