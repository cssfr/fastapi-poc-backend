"""
Seed script for populating the backtesting database with realistic dummy data.

Usage:
    python scripts/seed_data.py                  # Insert seed data
    python scripts/seed_data.py --clean          # Truncate all seeded tables first, then insert
    python scripts/seed_data.py --clean-only     # Truncate only, don't insert

Requires DATABASE_URL environment variable.
"""
import asyncio
import asyncpg
import argparse
import os
import sys
import json
import uuid
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal

# ---------------------------------------------------------------------------
# Existing users (from the live database)
# ---------------------------------------------------------------------------
USER_CHRISTIAN = "398e1588-2251-4cc3-a6b3-f01f5baeefdc"
USER_R = "ae797ec4-3dc1-427d-b7d8-5f0fd34b4bba"

# ---------------------------------------------------------------------------
# Helper: deterministic UUIDs (readable & reproducible across runs)
# ---------------------------------------------------------------------------
def make_id(prefix: str, n: int) -> str:
    return f"{prefix}-{n:04d}"

def ts(year, month, day, hour=0, minute=0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Data definitions
# ---------------------------------------------------------------------------

STRATEGY_DEFINITIONS = [
    {
        "user_id": USER_CHRISTIAN,
        "strategy_id": "momentum-breakout-v1",
        "strategy_class": "MomentumBreakout",
        "display_name": "Momentum Breakout",
        "description": "Enters on breakout above N-period high with ATR-based stops.",
        "schema_version": "1.0.0",
        "parameter_schema": json.dumps({
            "type": "object",
            "properties": {
                "lookback_period": {"type": "integer", "default": 20},
                "breakout_threshold": {"type": "number", "default": 1.5},
                "atr_multiplier": {"type": "number", "default": 2.0},
                "risk_per_trade": {"type": "number", "default": 0.01},
            },
        }),
    },
    {
        "user_id": USER_CHRISTIAN,
        "strategy_id": "mean-reversion-bb-v1",
        "strategy_class": "MeanReversionBollinger",
        "display_name": "Mean Reversion (Bollinger)",
        "description": "Fades Bollinger Band extremes when RSI confirms divergence.",
        "schema_version": "1.0.0",
        "parameter_schema": json.dumps({
            "type": "object",
            "properties": {
                "bb_period": {"type": "integer", "default": 20},
                "bb_std": {"type": "number", "default": 2.0},
                "rsi_period": {"type": "integer", "default": 14},
                "rsi_oversold": {"type": "integer", "default": 30},
                "rsi_overbought": {"type": "integer", "default": 70},
            },
        }),
    },
    {
        "user_id": USER_R,
        "strategy_id": "trend-follow-ema-v1",
        "strategy_class": "TrendFollowEMA",
        "display_name": "Trend Follower (EMA Cross)",
        "description": "Dual EMA crossover with ATR trailing stop.",
        "schema_version": "1.0.0",
        "parameter_schema": json.dumps({
            "type": "object",
            "properties": {
                "fast_ema": {"type": "integer", "default": 9},
                "slow_ema": {"type": "integer", "default": 21},
                "atr_period": {"type": "integer", "default": 14},
                "trail_atr_mult": {"type": "number", "default": 1.5},
            },
        }),
    },
]

STRATEGY_CONFIGURATIONS = [
    {
        "user_id": USER_CHRISTIAN,
        "config_id": "cfg-momentum-es-1h",
        "def_index": 0,
        "instance_name": "Momentum ES Hourly",
        "description": "Momentum breakout on ES hourly bars",
        "parameters": json.dumps({"lookback_period": 20, "breakout_threshold": 1.5, "atr_multiplier": 2.0, "risk_per_trade": 0.01}),
        "symbol": "ES",
        "timeframe": "1h",
    },
    {
        "user_id": USER_CHRISTIAN,
        "config_id": "cfg-momentum-dax-15m",
        "def_index": 0,
        "instance_name": "Momentum DAX 15min",
        "description": "Momentum breakout on DAX 15-minute bars",
        "parameters": json.dumps({"lookback_period": 14, "breakout_threshold": 1.2, "atr_multiplier": 2.5, "risk_per_trade": 0.005}),
        "symbol": "DAX",
        "timeframe": "15m",
    },
    {
        "user_id": USER_CHRISTIAN,
        "config_id": "cfg-meanrev-es-1h",
        "def_index": 1,
        "instance_name": "Mean Reversion ES Hourly",
        "description": "Bollinger mean reversion on ES hourly",
        "parameters": json.dumps({"bb_period": 20, "bb_std": 2.0, "rsi_period": 14, "rsi_oversold": 30, "rsi_overbought": 70}),
        "symbol": "ES",
        "timeframe": "1h",
    },
    {
        "user_id": USER_R,
        "config_id": "cfg-trend-dax-15m",
        "def_index": 2,
        "instance_name": "Trend Follow DAX 15min",
        "description": "EMA crossover trend follower on DAX 15min",
        "parameters": json.dumps({"fast_ema": 9, "slow_ema": 21, "atr_period": 14, "trail_atr_mult": 1.5}),
        "symbol": "DAX",
        "timeframe": "15m",
    },
    {
        "user_id": USER_R,
        "config_id": "cfg-trend-btc-4h",
        "def_index": 2,
        "instance_name": "Trend Follow BTC 4h",
        "description": "EMA crossover trend follower on BTC 4-hour bars",
        "parameters": json.dumps({"fast_ema": 12, "slow_ema": 26, "atr_period": 14, "trail_atr_mult": 2.0}),
        "symbol": "BTC",
        "timeframe": "4h",
    },
]

BACKTEST_RUNS = [
    {
        "user_id": USER_CHRISTIAN,
        "backtest_id": "bt-momentum-es-2025h1",
        "name": "Momentum ES H1 2025",
        "strategy_name": "MomentumBreakout",
        "config_index": 0,
        "symbol": "ES",
        "timeframe": "1h",
        "start_date": date(2025, 6, 1),
        "end_date": date(2025, 9, 30),
        "initial_balance": Decimal("50000.00"),
        "status": "completed",
        "execution_time_ms": 4523,
        "data_source": "1Y",
        "data_points_count": 2040,
    },
    {
        "user_id": USER_CHRISTIAN,
        "backtest_id": "bt-meanrev-es-2025q4",
        "name": "Mean Reversion ES Q4 2025",
        "strategy_name": "MeanReversionBollinger",
        "config_index": 2,
        "symbol": "ES",
        "timeframe": "1h",
        "start_date": date(2025, 10, 1),
        "end_date": date(2025, 12, 31),
        "initial_balance": Decimal("50000.00"),
        "status": "completed",
        "execution_time_ms": 3180,
        "data_source": "1Y",
        "data_points_count": 1560,
    },
    {
        "user_id": USER_R,
        "backtest_id": "bt-trend-dax-2025h2",
        "name": "Trend Follow DAX H2 2025",
        "strategy_name": "TrendFollowEMA",
        "config_index": 3,
        "symbol": "DAX",
        "timeframe": "15m",
        "start_date": date(2025, 8, 1),
        "end_date": date(2025, 10, 31),
        "initial_balance": Decimal("25000.00"),
        "status": "completed",
        "execution_time_ms": 8912,
        "data_source": "1Y",
        "data_points_count": 8640,
    },
    {
        "user_id": USER_CHRISTIAN,
        "backtest_id": "bt-momentum-es-2026",
        "name": "Momentum ES 2026 (running)",
        "strategy_name": "MomentumBreakout",
        "config_index": 0,
        "symbol": "ES",
        "timeframe": "1h",
        "start_date": date(2026, 1, 5),
        "end_date": date(2026, 2, 15),
        "initial_balance": Decimal("50000.00"),
        "status": "running",
        "execution_time_ms": None,
        "data_source": "1Y",
        "data_points_count": None,
    },
]

# ---------------------------------------------------------------------------
# Trade specs: (side, entry_price, exit_price, exit_reason,
#               entry_dt, exit_dt, sl_distance, tp_distance)
#
# Derived fields (session_high/low, pnl, mfe, mae, holding) are computed
# automatically by _build_trade() below.
# ---------------------------------------------------------------------------

TRADES_BY_RUN = {
    0: [  # Run 0: Momentum ES (prices ~5800)
        ("long",  5820, 5865, "take_profit",    ts(2025,6,12,14),  ts(2025,6,12,19),  25, 50),
        ("long",  5840, 5815, "stop_loss",      ts(2025,6,25,10),  ts(2025,6,25,13),  25, 50),
        ("short", 5870, 5835, "signal_exit",    ts(2025,7,8,15),   ts(2025,7,9,11),   30, 60),
        ("long",  5810, 5850, "trailing_stop",  ts(2025,7,22,9),   ts(2025,7,23,14),  20, 60),
        ("short", 5900, 5920, "stop_loss",      ts(2025,8,5,11),   ts(2025,8,5,14),   20, 50),
        ("long",  5850, 5890, "take_profit",    ts(2025,9,15,10),  ts(2025,9,16,10),  30, 45),
    ],
    1: [  # Run 1: Mean Reversion ES (prices ~6050)
        ("long",  6050, 6090, "take_profit",    ts(2025,10,7,11),  ts(2025,10,7,17),  30, 45),
        ("short", 6100, 6065, "signal_exit",    ts(2025,10,21,14), ts(2025,10,22,10), 35, 60),
        ("long",  6020, 5995, "stop_loss",      ts(2025,11,4,9),   ts(2025,11,4,12),  25, 50),
        ("short", 6080, 6110, "stop_loss",      ts(2025,11,19,13), ts(2025,11,19,16), 30, 55),
        ("long",  6040, 6085, "take_profit",    ts(2025,12,10,10), ts(2025,12,11,9),  25, 50),
    ],
    2: [  # Run 2: Trend Follow DAX (prices ~18500)
        ("long",  18450, 18520, "signal_exit",   ts(2025,8,11,10), ts(2025,8,12,15),  80, 120),
        ("long",  18500, 18440, "stop_loss",     ts(2025,8,26,14), ts(2025,8,26,16),  60, 100),
        ("short", 18600, 18530, "trailing_stop", ts(2025,9,9,11),  ts(2025,9,10,14),  70, 120),
        ("long",  18480, 18560, "take_profit",   ts(2025,10,6,9),  ts(2025,10,7,11),  60, 90),
    ],
}


def _build_trade(run_idx: int, trade_num: int, spec: tuple, backtest_run_id: int) -> dict:
    """Compute all derived fields from a trade spec, ensuring financial consistency."""
    side, entry_px, exit_px, exit_reason, entry_dt, exit_dt, sl_dist, tp_dist = spec

    is_long = side == "long"
    pnl = (exit_px - entry_px) if is_long else (entry_px - exit_px)

    if is_long:
        initial_sl = entry_px - sl_dist
        initial_tp = entry_px + tp_dist
        session_high = max(entry_px, exit_px) + abs(pnl) * 0.3
        session_low = min(entry_px, exit_px, initial_sl) - abs(pnl) * 0.1
        mfe_points = session_high - entry_px
        mae_points = entry_px - session_low
        mfe_price = session_high
        mae_price = session_low
    else:
        initial_sl = entry_px + sl_dist
        initial_tp = entry_px - tp_dist
        session_low = min(entry_px, exit_px) - abs(pnl) * 0.3
        session_high = max(entry_px, exit_px, initial_sl) + abs(pnl) * 0.1
        mfe_points = entry_px - session_low
        mae_points = session_high - entry_px
        mfe_price = session_low
        mae_price = session_high

    holding_minutes = int((exit_dt - entry_dt).total_seconds() / 60)
    mfe_dt = entry_dt + (exit_dt - entry_dt) * 0.6
    mae_dt = entry_dt + (exit_dt - entry_dt) * 0.3

    final_sl = initial_sl
    if exit_reason == "trailing_stop":
        if is_long:
            final_sl = exit_px
        else:
            final_sl = exit_px

    return {
        "backtest_run_id": backtest_run_id,
        "trade_id": make_id(f"r{run_idx}-t", trade_num),
        "symbol": BACKTEST_RUNS[run_idx]["symbol"],
        "trade_date": entry_dt.date(),
        "side": side,
        "entry_timestamp": entry_dt,
        "entry_bar_index": trade_num * 40 + 10,
        "entry_price": Decimal(str(entry_px)),
        "exit_timestamp": exit_dt,
        "exit_bar_index": trade_num * 40 + 10 + max(1, holding_minutes // 60),
        "exit_price": Decimal(str(exit_px)),
        "exit_reason": exit_reason,
        "initial_stop_loss": Decimal(str(initial_sl)),
        "initial_take_profit": Decimal(str(initial_tp)),
        "final_stop_loss": Decimal(str(final_sl)),
        "final_take_profit": Decimal(str(initial_tp)),
        "pnl_points": Decimal(str(pnl)),
        "session_high": Decimal(str(round(session_high, 2))),
        "session_low": Decimal(str(round(session_low, 2))),
        "holding_period_minutes": holding_minutes,
        "holding_period_bars": max(1, holding_minutes // 60),
        "entry_slippage_points": Decimal("0.25"),
        "exit_slippage_points": Decimal("0.50"),
        "strategy_context": json.dumps({"signal_strength": round(0.6 + trade_num * 0.05, 2), "regime": "trending" if pnl > 0 else "choppy"}),
        "mfe_points": Decimal(str(round(mfe_points, 2))),
        "mfe_price": Decimal(str(round(mfe_price, 2))),
        "mfe_timestamp": mfe_dt,
        "mae_points": Decimal(str(round(mae_points, 2))),
        "mae_price": Decimal(str(round(mae_price, 2))),
        "mae_timestamp": mae_dt,
    }


def _build_events_for_trade(trade: dict) -> list[dict]:
    """Generate realistic order ledger events for a single trade."""
    entry_dt = trade["entry_timestamp"]
    exit_dt = trade["exit_timestamp"]
    run_id = trade["backtest_run_id"]
    tid = trade["trade_id"]
    side = trade["side"]
    order_side = "buy" if side == "long" else "sell"
    close_side = "sell" if side == "long" else "buy"

    events = [
        {
            "backtest_run_id": run_id,
            "trade_id": tid,
            "event_timestamp": entry_dt - timedelta(seconds=5),
            "event_sequence": 1,
            "event_type": "entry_order",
            "order_type": "limit",
            "order_side": order_side,
            "order_quantity": Decimal("1"),
            "order_price": trade["entry_price"],
        },
        {
            "backtest_run_id": run_id,
            "trade_id": tid,
            "event_timestamp": entry_dt,
            "event_sequence": 2,
            "event_type": "entry_fill",
            "order_type": "limit",
            "order_side": order_side,
            "fill_price": trade["entry_price"] + Decimal("0.25"),
            "fill_quantity": Decimal("1"),
            "slippage_bps": Decimal("0.43"),
        },
    ]

    seq = 3
    if trade["exit_reason"] == "trailing_stop":
        adj_dt = entry_dt + (exit_dt - entry_dt) * 0.5
        events.append({
            "backtest_run_id": run_id,
            "trade_id": tid,
            "event_timestamp": adj_dt,
            "event_sequence": seq,
            "event_type": "stop_adjusted",
            "previous_stop_loss": trade["initial_stop_loss"],
            "new_stop_loss": trade["final_stop_loss"],
            "stop_loss_reason": "trailing",
        })
        seq += 1

    events.append({
        "backtest_run_id": run_id,
        "trade_id": tid,
        "event_timestamp": exit_dt - timedelta(seconds=2),
        "event_sequence": seq,
        "event_type": "exit_order",
        "order_type": "market" if trade["exit_reason"] in ("stop_loss", "trailing_stop") else "limit",
        "order_side": close_side,
        "order_quantity": Decimal("1"),
        "order_price": trade["exit_price"],
    })
    seq += 1

    events.append({
        "backtest_run_id": run_id,
        "trade_id": tid,
        "event_timestamp": exit_dt,
        "event_sequence": seq,
        "event_type": "exit_fill",
        "order_side": close_side,
        "fill_price": trade["exit_price"] + Decimal("0.50"),
        "fill_quantity": Decimal("1"),
        "slippage_bps": Decimal("0.85"),
        "tag": trade["exit_reason"],
    })

    return events


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

TABLES_IN_FK_ORDER = [
    "trade_events",
    "backtest_trade_results",
    "backtest_runs",
    "strategy_configurations",
    "strategy_definitions",
]


async def clean_tables(conn):
    """Truncate all seeded tables in reverse FK order."""
    print("\n--- Cleaning tables ---")
    for table in TABLES_IN_FK_ORDER:
        await conn.execute(f"TRUNCATE TABLE {table} CASCADE")
        print(f"  Truncated {table}")
    print("  Done.\n")


async def insert_strategy_definitions(conn) -> list[int]:
    """Returns list of generated IDs in insertion order."""
    ids = []
    for sd in STRATEGY_DEFINITIONS:
        row_id = await conn.fetchval("""
            INSERT INTO strategy_definitions
                (user_id, strategy_id, strategy_class, display_name,
                 description, schema_version, parameter_schema)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            RETURNING id
        """, uuid.UUID(sd["user_id"]), sd["strategy_id"], sd["strategy_class"],
            sd["display_name"], sd["description"], sd["schema_version"],
            sd["parameter_schema"])
        ids.append(row_id)
    print(f"  Inserted {len(ids)} strategy_definitions")
    return ids


async def insert_strategy_configurations(conn, def_ids: list[int]) -> list[int]:
    ids = []
    for sc in STRATEGY_CONFIGURATIONS:
        row_id = await conn.fetchval("""
            INSERT INTO strategy_configurations
                (user_id, config_id, strategy_definition_id, instance_name,
                 description, parameters, symbol, timeframe)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            RETURNING id
        """, uuid.UUID(sc["user_id"]), sc["config_id"], def_ids[sc["def_index"]],
            sc["instance_name"], sc["description"], sc["parameters"],
            sc["symbol"], sc["timeframe"])
        ids.append(row_id)
    print(f"  Inserted {len(ids)} strategy_configurations")
    return ids


async def insert_backtest_runs(conn, config_ids: list[int]) -> list[int]:
    ids = []
    for br in BACKTEST_RUNS:
        params = STRATEGY_CONFIGURATIONS[br["config_index"]]["parameters"]
        completed_at = None
        if br["status"] == "completed":
            completed_at = datetime.combine(br["end_date"], datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(hours=1)

        row_id = await conn.fetchval("""
            INSERT INTO backtest_runs
                (user_id, backtest_id, name, strategy_name, strategy_configuration_id,
                 strategy_parameters, symbol, timeframe, start_date, end_date,
                 initial_balance, status, completed_at, execution_time_ms,
                 data_source, data_points_count, total_trades)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17)
            RETURNING id
        """, uuid.UUID(br["user_id"]), br["backtest_id"], br["name"],
            br["strategy_name"], config_ids[br["config_index"]],
            params, br["symbol"], br["timeframe"],
            br["start_date"], br["end_date"],
            br["initial_balance"], br["status"], completed_at,
            br["execution_time_ms"], br["data_source"], br["data_points_count"],
            len(TRADES_BY_RUN.get(BACKTEST_RUNS.index(br), [])))
        ids.append(row_id)
    print(f"  Inserted {len(ids)} backtest_runs")
    return ids


async def insert_trades_and_events(conn, run_ids: list[int]):
    trade_count = 0
    event_count = 0

    for run_idx, specs in TRADES_BY_RUN.items():
        for trade_num, spec in enumerate(specs, start=1):
            trade = _build_trade(run_idx, trade_num, spec, run_ids[run_idx])

            await conn.execute("""
                INSERT INTO backtest_trade_results
                    (backtest_run_id, trade_id, symbol, trade_date, side,
                     entry_timestamp, entry_bar_index, entry_price,
                     exit_timestamp, exit_bar_index, exit_price, exit_reason,
                     initial_stop_loss, initial_take_profit,
                     final_stop_loss, final_take_profit,
                     pnl_points, session_high, session_low,
                     holding_period_minutes, holding_period_bars,
                     entry_slippage_points, exit_slippage_points,
                     strategy_context,
                     mfe_points, mfe_price, mfe_timestamp,
                     mae_points, mae_price, mae_timestamp)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                        $17,$18,$19,$20,$21,$22,$23,$24::jsonb,$25,$26,$27,$28,$29,$30)
            """,
                trade["backtest_run_id"], trade["trade_id"], trade["symbol"],
                trade["trade_date"], trade["side"],
                trade["entry_timestamp"], trade["entry_bar_index"], trade["entry_price"],
                trade["exit_timestamp"], trade["exit_bar_index"], trade["exit_price"],
                trade["exit_reason"],
                trade["initial_stop_loss"], trade["initial_take_profit"],
                trade["final_stop_loss"], trade["final_take_profit"],
                trade["pnl_points"], trade["session_high"], trade["session_low"],
                trade["holding_period_minutes"], trade["holding_period_bars"],
                trade["entry_slippage_points"], trade["exit_slippage_points"],
                trade["strategy_context"],
                trade["mfe_points"], trade["mfe_price"], trade["mfe_timestamp"],
                trade["mae_points"], trade["mae_price"], trade["mae_timestamp"],
            )
            trade_count += 1

            events = _build_events_for_trade(trade)
            for ev in events:
                await conn.execute("""
                    INSERT INTO trade_events
                        (backtest_run_id, trade_id, event_timestamp, event_sequence,
                         event_type, order_type, order_side, order_quantity, order_price,
                         fill_price, fill_quantity, slippage_bps,
                         previous_stop_loss, new_stop_loss, stop_loss_reason, tag)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                """,
                    ev.get("backtest_run_id"), ev.get("trade_id"),
                    ev.get("event_timestamp"), ev.get("event_sequence"),
                    ev.get("event_type"), ev.get("order_type"), ev.get("order_side"),
                    ev.get("order_quantity"), ev.get("order_price"),
                    ev.get("fill_price"), ev.get("fill_quantity"),
                    ev.get("slippage_bps"),
                    ev.get("previous_stop_loss"), ev.get("new_stop_loss"),
                    ev.get("stop_loss_reason"), ev.get("tag"),
                )
                event_count += 1

    print(f"  Inserted {trade_count} backtest_trade_results")
    print(f"  Inserted {event_count} trade_events")
    return trade_count, event_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="Seed the backtesting database")
    parser.add_argument("--clean", action="store_true", help="Truncate seeded tables before inserting")
    parser.add_argument("--clean-only", action="store_true", help="Truncate tables and exit")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)

    conn = await asyncpg.connect(db_url)
    print(f"Connected to database.")

    try:
        async with conn.transaction():
            if args.clean or args.clean_only:
                await clean_tables(conn)
                if args.clean_only:
                    print("Clean-only mode — done.")
                    return

            print("\n--- Inserting seed data ---")
            def_ids = await insert_strategy_definitions(conn)
            config_ids = await insert_strategy_configurations(conn, def_ids)
            run_ids = await insert_backtest_runs(conn, config_ids)
            trade_count, event_count = await insert_trades_and_events(conn, run_ids)

            print("\n--- Summary ---")
            print(f"  strategy_definitions:     {len(def_ids)}")
            print(f"  strategy_configurations:  {len(config_ids)}")
            print(f"  backtest_runs:            {len(run_ids)}")
            print(f"  backtest_trade_results:   {trade_count}")
            print(f"  trade_events:             {event_count}")
            print(f"\nAll inserts committed in a single transaction.")

    finally:
        await conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
