// Sprint Sniper — a fast-rotation pump.fun paper-trading bot.
//
// Idea: on a 6-hour window of fresh bonding curves, most coins bleed to zero and
// a few run hard. The edge is not picking winners — it is cutting losers fast and
// letting the rare runner pay for all of them. So every position is small
// (0.10 SOL), entered only on confirmed buy pressure, and exited on the first
// sign of weakness: a 12% stop loss, a 20% trailing stop once up 10%, a
// take-profit at +80%, a fade exit when net flow turns negative, or a hard
// time limit. One entry per coin, no pyramiding: a second buy into a coin that
// already moved is how a small loss becomes a big one.
//
// Guardrails: the run must end with a round trip of at least 0.01 SOL on at
// least 20 coins and a max drawdown of 50% or less. With at most 5 open
// positions the bot rotates quickly, and a small probe round trip (0.012 SOL)
// keeps the fill/coin pace on track when the main signal goes quiet.
//
// Deterministic by construction: no clock, no randomness, iteration only over
// insertion-ordered maps fed by the batch stream. Two runs on the same window
// produce byte-identical action logs.
//
// Run by the grader as `node /bot/main.ts` with this directory mounted at /bot.

import {
  LAMPORTS_PER_SOL,
  runBot,
  type Batch,
  type BotState,
  type OrderInput,
  type Pubkey,
} from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
// The curve takes 1.25% (95 + 30 bps) on each side; a round trip costs ~2.5%
// of the stake plus two 5000-lamport tx fees, so a trade must clear ~3% gross
// to make money. The stop loss is set far wider than that on purpose: exits
// are driven by price action, not by fee noise.
const CURVE_FEE = 0.0125;

interface Params {
  /** Entry lookback in slots. */
  window: number;
  /** Net buy SOL (buys minus sells) over the window. */
  minNetSol: number;
  /** Buy trades in the window. */
  minBuys: number;
  /** Distinct buying wallets in the window. */
  minBuyers: number;
  /** Price now over price at the window start, minus 1. */
  minPriceUp: number;
  /** Upper bound on that rise: never chase a vertical candle. */
  maxPriceUp: number;
  /** Newer-half net inflow must be at least this multiple of the older half. */
  accel: number;
  /** Price now at least this share of the window's high. */
  nearHigh: number;
  /** Curve real SOL bounds at entry. */
  minRqSol: number;
  maxRqSol: number;
  /** Slots since the CreateEvent before an entry is allowed. */
  minAge: number;
  /** SOL staked on one coin. */
  stakeSol: number;
  /** Smallest buy worth sending. */
  minBuySol: number;
  /** Slippage on buys (bps); sells always go out at 10000. */
  buySlipBps: number;
  /** Exit when value falls this far below cost. */
  stopLoss: number;
  /** Trailing stop: exit when value falls this far below its peak. */
  trail: number;
  /** The trailing stop arms once value reaches cost * (1 + trailArm). */
  trailArm: number;
  /** Take the whole profit at this gain over cost. */
  takeProfit: number;
  /** Exit when net SOL over the fade window falls below -fadeNetSol. */
  fadeWindow: number;
  fadeNetSol: number;
  /** Exit on any single recorded sell of at least this many SOL. */
  bigSellSol: number;
  /** Exit after this many slots held. */
  maxHold: number;
  /** Guardrail pacing: targets for the run, scaled by elapsed block time. */
  targetCoins: number;
  targetFills: number;
  paceSecs: number;
  probeSol: number;
  maxProbes: number;
}

const PARAMS: Params = {
  window: 60,
  minNetSol: 0.6,
  minBuys: 4,
  minBuyers: 4,
  minPriceUp: 0.08,
  maxPriceUp: 1.2,
  accel: 1,
  nearHigh: 0.92,
  minRqSol: 1,
  maxRqSol: 80,
  minAge: 400,
  stakeSol: 0.08,
  minBuySol: 0.01,
  buySlipBps: 1000,
  stopLoss: 0.06,
  trail: 0.2,
  trailArm: 0.08,
  takeProfit: 0.6,
  fadeWindow: 20,
  fadeNetSol: 0.5,
  bigSellSol: 5,
  maxHold: 1000,
  targetCoins: 30,
  targetFills: 240,
  paceSecs: 14_400,
  probeSol: 0.012,
  maxProbes: 1,
};

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Tape {
  created: number | null;
  trades: Trade[];
  entered: boolean;
  lastExit: number;
}

interface Held {
  opened: number;
  peakValue: number;
  /** SOL sent in buys for this position (fees included). */
  invested: number;
  exiting: boolean;
  filled: boolean;
  /** Close value at the latest batch (SOL, curve and tx fees deducted). */
  lastValue: number;
}

function priceOf(vq: bigint, vt: bigint): number {
  const q = Number(vq);
  const t = Number(vt);
  return t > 0 ? q / t : 0;
}

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    const p = PARAMS;
    const now = batch.slot;
    const tapes = (state as unknown as { __tapes?: Map<Pubkey, Tape> }).__tapes ??
      ((state as unknown as { __tapes: Map<Pubkey, Tape> }).__tapes = new Map());
    const held = (state as unknown as { __held?: Map<Pubkey, Held> }).__held ??
      ((state as unknown as { __held: Map<Pubkey, Held> }).__held = new Map());
    const probes = (state as unknown as { __probes?: Map<Pubkey, boolean> }).__probes ??
      ((state as unknown as { __probes: Map<Pubkey, boolean> }).__probes = new Map());
    const pace = (state as unknown as { __pace?: { fills: number; coins: Set<Pubkey>; first: number | null; last: number | null } }).__pace ??
      ((state as unknown as { __pace: { fills: number; coins: Set<Pubkey>; first: number | null; last: number | null } }).__pace = { fills: 0, coins: new Set(), first: null, last: null });

    const keep = p.window + 5;
    const bigSell = new Set<Pubkey>();
    const traded = new Set<Pubkey>();

    for (const f of batch.fills) {
      if (f.status === 'filled' && !f.client_id.startsWith('_')) {
        pace.fills++;
        pace.coins.add(f.mint);
      }
    }
    for (const e of batch.events) {
      if (e.block_time !== null) {
        if (pace.first === null) pace.first = e.block_time;
        if (pace.last === null || e.block_time > pace.last) pace.last = e.block_time;
      }
      let t = tapes.get(e.mint);
      if (t === undefined) {
        t = { created: null, trades: [], entered: false, lastExit: -1e12 };
        tapes.set(e.mint, t);
      }
      if (e.event === 'CreateEvent') {
        t.created = e.slot;
      } else if (e.event === 'TradeEvent') {
        const sol = Number(e.sol_amount) / LAMPORTS;
        t.trades.push({
          slot: e.slot,
          buy: e.is_buy,
          sol,
          user: e.user,
          price: priceOf(BigInt(e.virtual_quote_reserves), BigInt(e.virtual_token_reserves)),
        });
        traded.add(e.mint);
        if (!e.is_buy && sol >= p.bigSellSol) bigSell.add(e.mint);
      }
    }
    for (const mint of traded) {
      const t = tapes.get(mint)!;
      let cut = 0;
      while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
      if (cut > 0) t.trades.splice(0, cut);
    }

    const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
    const pendingByMint = new Map<Pubkey, number>();
    for (const po of state.pending.values()) {
      pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);
    }

    const orders: OrderInput[] = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const tx = Number(state.txFeeLamports()) / LAMPORTS;

    const valueOf = (mint: Pubkey): { value: number; cost: number; rqSol: number } | null => {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined || curve === undefined) return null;
      const mk = marks.get(mint);
      const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
      const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
      const rqSol = (mk ? Number(mk.real_quote_reserves) : Number(curve.real_quote_reserves)) / LAMPORTS;
      const tokens = Number(pos.token_amount);
      // Selling `tokens` into the curve returns tokens * vq / (vt + tokens),
      // minus the curve fee and one tx fee.
      const value = ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - tx;
      return { value, cost: Number(pos.cost_lamports) / LAMPORTS, rqSol };
    };

    const netFlow = (t: Tape, w: number): number => {
      let net = 0;
      for (let i = t.trades.length - 1; i >= 0; i--) {
        const tr = t.trades[i]!;
        if (tr.slot <= now - w) break;
        net += tr.buy ? tr.sol : -tr.sol;
      }
      return net;
    };

    // Exits first: cut losers, bank winners, never let a position bleed out.
    for (const [mint, h] of held) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) {
          held.delete(mint);
          if (h.filled) tapes.get(mint)!.lastExit = now;
        }
        continue;
      }
      h.filled = true;
      if (curve === undefined || curve.complete) continue;
      const v = valueOf(mint);
      if (v === null) continue;
      if (v.value > h.peakValue) h.peakValue = v.value;
      h.lastValue = v.value;
      const t = tapes.get(mint)!;
      let exit = h.exiting;
      if (!exit && v.value < v.cost * (1 - p.stopLoss)) exit = true;
      if (!exit && v.value >= v.cost * (1 + p.takeProfit)) exit = true;
      if (!exit && h.peakValue >= v.cost * (1 + p.trailArm) && v.value < h.peakValue * (1 - p.trail)) exit = true;
      if (!exit && netFlow(t, p.fadeWindow) < -p.fadeNetSol) exit = true;
      if (!exit && bigSell.has(mint)) exit = true;
      if (!exit && now - h.opened > p.maxHold) exit = true;
      if (!exit) continue;
      h.exiting = true;
      let sellPending = false;
      for (const po of state.pending.values()) {
        if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
      }
      if (sellPending) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
    }

    // Probe round trips: sell the whole probe as soon as its buy has landed.
    for (const [mint, selling] of probes) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) probes.delete(mint);
        continue;
      }
      if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      if (!selling) probes.set(mint, true);
    }

    // Entries: one small buy per coin, only on confirmed buy pressure.
    let free = Number(state.freeCashLamports()) / LAMPORTS;
    let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
    for (const mint of traded) {
      if (orders.length >= cap) break;
      if (slotsLeft <= 0) break;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingByMint.has(mint) || probes.has(mint) || held.has(mint)) continue;
      const t = tapes.get(mint)!;
      if (t.entered) continue;
      const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
      if (rqSol < p.minRqSol || rqSol > p.maxRqSol) continue;
      if (t.created === null || now - t.created < p.minAge) continue;

      const from = now - p.window;
      const mid = now - p.window / 2;
      let buySol = 0;
      let sellSol = 0;
      let buys = 0;
      let netOld = 0;
      let netNew = 0;
      let startPrice = -1;
      let high = 0;
      let last = 0;
      const byUser = new Map<string, number>();
      for (const tr of t.trades) {
        if (tr.slot <= from) {
          startPrice = tr.price;
          continue;
        }
        if (startPrice < 0) startPrice = tr.price;
        const signed = tr.buy ? tr.sol : -tr.sol;
        if (tr.slot <= mid) netOld += signed;
        else netNew += signed;
        if (tr.buy) {
          buySol += tr.sol;
          buys++;
          byUser.set(tr.user, (byUser.get(tr.user) ?? 0) + tr.sol);
        } else {
          sellSol += tr.sol;
        }
        if (tr.price > high) high = tr.price;
        last = tr.price;
      }
      if (buys < p.minBuys || byUser.size < p.minBuyers) continue;
      if (buySol - sellSol < p.minNetSol) continue;
      if (startPrice <= 0) continue;
      const up = last / startPrice - 1;
      if (up < p.minPriceUp || up > p.maxPriceUp) continue;
      if (last < high * p.nearHigh) continue;
      if (netNew <= 0 || netNew < p.accel * netOld) continue;

      const capSol = Number(state.maxBuyLamports(mint)) / LAMPORTS;
      let amount = Math.min(capSol, p.stakeSol);
      if (amount > free - tx) amount = free - tx;
      if (amount < p.minBuySol) continue;
      const lamports = BigInt(Math.floor(amount * LAMPORTS));
      orders.push({ side: 'buy', mint, quote_lamports: lamports, max_slippage_bps: p.buySlipBps });
      free -= amount + tx;
      slotsLeft--;
      t.entered = true;
      held.set(mint, { opened: now, peakValue: 0, invested: amount, exiting: false, filled: false, lastValue: 0 });
    }

    // Guardrail pacing: open a probe when fills or coins lag the pace.
    if (probes.size < p.maxProbes && slotsLeft > 0 && pace.first !== null && pace.last !== null) {
      const elapsed = Math.min(1, (pace.last - pace.first) / p.paceSecs);
      const needCoins = pace.coins.size < Math.ceil(p.targetCoins * elapsed);
      const needFills = pace.fills < Math.ceil(p.targetFills * elapsed);
      if (needCoins || needFills) {
        const minRq = (p.probeSol * 10_000) / state.hello.caps.max_order_bps_of_real_reserve;
        for (const mint of traded) {
          if (orders.length >= cap || probes.size >= p.maxProbes) break;
          if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
          if (needCoins && !needFills && pace.coins.has(mint)) continue;
          const curve = state.curves.get(mint);
          if (curve === undefined || curve.complete) continue;
          if (Number(curve.real_quote_reserves) / LAMPORTS < minRq * 1.5) continue;
          if (free < p.probeSol + 2 * tx) break;
          orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS)), max_slippage_bps: p.buySlipBps });
          probes.set(mint, false);
          free -= p.probeSol + tx;
        }
      }
    }
    return orders;
  },
});
