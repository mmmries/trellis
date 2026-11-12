## base
| shape | n | plain INSERT s | CTAS s | Trellis build s | build ratio vs INSERT | plain WAL | build WAL | state (trellis schema) | target | verify (cnt, plain cnt, diff, diff) |
|---|---|---|---|---|---|---|---|---|---|---|
| s2_agg | 3 | 0.6 (0.5–0.6) | 0.5 (0.5–0.6) | 6.8 (6.8–6.8) | 11.3 (10.6–12.9) | 16 MB | 81 MB | 29 MB | 9 MB | OK 99995/99995/0/0 |
| s2_agg_hi | 3 | 1.3 (1.3–1.5) | 0.8 (0.8–0.9) | 19.4 (19.1–21.1) | 14.8 (14.3–15.0) | 155 MB | 917 MB | 278 MB | 83 MB | OK 1000000/1000000/0/0 |
## proto
| shape | n | plain INSERT s | CTAS s | Trellis build s | build ratio vs INSERT | plain WAL | build WAL | state (trellis schema) | target | verify (cnt, plain cnt, diff, diff) |
|---|---|---|---|---|---|---|---|---|---|---|
| s2_agg | 3 | 0.6 (0.6–0.7) | 0.5 (0.5–1.2) | 6.6 (6.2–6.6) | 10.6 (8.6–10.9) | 16 MB | 48 MB | 16 MB | 9 MB | OK 99995/99995/0/0 |
| s2_agg_hi | 3 | 1.3 (1.3–1.3) | 0.8 (0.8–0.8) | 11.9 (11.8–11.9) | 9.1 (8.8–9.3) | 155 MB | 559 MB | 149 MB | 83 MB | OK 1000000/1000000/0/0 |
## proto10m
| shape | n | plain INSERT s | CTAS s | Trellis build s | build ratio vs INSERT | plain WAL | build WAL | state (trellis schema) | target | verify (cnt, plain cnt, diff, diff) |
|---|---|---|---|---|---|---|---|---|---|---|
| s2_agg_hi | 1 | 13.5 | 12.5 | 81.7 | 6.0 | 1546 MB | 5893 MB | 1485 MB | 827 MB | OK 10000000/10000000/0/0 |
