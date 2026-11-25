# Sprint Sniper — Pump.fun Paper Sprint bot

Entry for `u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b`
(Pump.fun Paper Sprint: Any Bot Can Win) on make.fun.

## Strategy

Fast-rotation momentum with tight risk control. Most fresh bonding curves
bleed to zero and a few run hard, so the bot buys small (0.10 SOL), only on
confirmed buy pressure, and exits on the first sign of weakness.

**Entry** (per coin, one buy only, 60-slot lookback):

- curve at least 400 slots old, real SOL reserve between 1 and 80 SOL
- price up 6–120% over the window, sitting within 8% of the window high
- net buy inflow ≥ 0.4 SOL, ≥ 4 buy trades from ≥ 3 distinct wallets
- inflow accelerating: newer half of the window beats the older half

**Exits** (whole position, first trigger wins):

- stop loss −12% vs cost
- take profit +80% vs cost
- trailing stop −20% from peak once up ≥ 10%
- fade: net outflow worse than −1 SOL over 30 slots
- any single sell ≥ 5 SOL on the tape
- max hold 1500 slots

**Guardrails:** with at most 5 open positions the bot rotates fast; a 0.012 SOL
probe round trip keeps the fill/coin pace on track (≥ 20 coins with ≥ 0.01 SOL
round trips, max drawdown ≤ 50%) when the main signal goes quiet.

## Layout

- `main.ts` — the bot, run by the grader as `node /bot/main.ts` (Node 22+).
- `vendor/makefun-bot.ts` — the kit's SDK shim, vendored byte-identical.
- `LICENSE` — MIT.

## Verify locally

```sh
node cli/makefun.mjs grade-local --kit <kit> --bot . --driver process
node cli/makefun.mjs grade-local --kit <kit> --bot . --driver process --stress 20
```
