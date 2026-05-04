from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "temp" / "combined_netlify"

STRATEGY3_PAYLOAD_PATH = ROOT / "temp" / "strategy3_payload.json"
STRATEGY3_ENGINE_PATH = ROOT / "temp" / "search_strategy3_fixedrisk.py"

S1_FLAT_SUMMARY_PATH = ROOT / "strategies" / "combined_netlify" / "15min" / "summary_buf005.json"
S1_COMPOUND_SUMMARY_PATH = ROOT / "strategies" / "combined_netlify" / "15min" / "summary_buf005_compound.json"
S1_FLAT_DATA_DIR = ROOT / "strategies" / "combined_netlify" / "15min" / "data_buf005"
S1_COMPOUND_DATA_DIR = ROOT / "strategies" / "combined_netlify" / "15min" / "data_buf005_compound"

S2_FLAT_SUMMARY_PATH = ROOT / "strategies" / "combined_netlify" / "5min_mr" / "summary_v1.json"
S2_COMPOUND_SUMMARY_PATH = ROOT / "strategies" / "combined_netlify" / "5min_mr" / "summary_v1_compound.json"
S2_FLAT_DATA_DIR = ROOT / "strategies" / "combined_netlify" / "5min_mr" / "data_v1"
S2_COMPOUND_DATA_DIR = ROOT / "strategies" / "combined_netlify" / "5min_mr" / "data_v1_compound"

S1_4V1_VARIANT_ID = "4v1"
S1_4V1_DATA_DIR = ROOT / "strategies" / "15min_opening_reversal" / "website" / "data_4thv2"
S1_LEGACY_DASHBOARD_PATHS = (
    ROOT / "temp" / "_base_dashboard_template.html",
    ROOT / "temp" / "index.html",
)

LEGACY_ORIGINAL_TP1_SHARE = 0.30

TRADE_LOG_LIMIT = 2000


@dataclass(frozen=True)
class ExitSpec:
    id: str
    short: str
    name: str


@dataclass(frozen=True)
class LeverageSpec:
    id: str
    lev: float
    mode: str
    short: str
    label: str


@dataclass(frozen=True)
class BaseConfig:
    id: str
    capital: float
    risk_pct: float
    short: str
    label: str


@dataclass(frozen=True)
class FullConfig:
    id: str
    short: str
    label: str
    base_id: str
    base_capital: float
    base_risk_pct: float
    capital: float
    risk_pct: float
    lev_id: str
    lev_mode: str
    lev: float


@dataclass(frozen=True)
class StrategyMeta:
    id: str
    title: str
    subtitle: str
    source_engine: str
    max_trades: int


@dataclass(frozen=True)
class PrecomputedPlan:
    strategy_id: str
    variant_id: str
    timeframe: str
    summary_flat_path: Path
    summary_compound_path: Path
    data_flat_dir: Path
    data_compound_dir: Path


@dataclass(frozen=True)
class Strategy3Context:
    title: str
    subtitle: str
    source_engine: str
    max_trades_per_day: int
    setups: tuple[dict[str, Any], ...]


EXIT_SPECS = [
    ExitSpec(id="e1", short="E1 50/50 CTC", name="E1 - TP1 50% / TP2 50% / SL->CTC"),
    ExitSpec(id="e4", short="E4 70% EOD", name="E4 - TP1 70% / SL Original / EOD"),
    ExitSpec(id="e8", short="E8 30% EOD", name="E8 - TP1 30% / SL Original / EOD"),
]


LEVERAGE_SPECS = [
    LeverageSpec(id="1x", lev=1.0, mode="none", short="1x", label="1x"),
    LeverageSpec(id="2xS", lev=2.0, mode="scaled", short="2x S", label="2x Scaled"),
    LeverageSpec(id="3xS", lev=3.0, mode="scaled", short="3x S", label="3x Scaled"),
    LeverageSpec(id="2xO", lev=2.0, mode="orig", short="2x O", label="2x Orig-Risk"),
    LeverageSpec(id="3xO", lev=3.0, mode="orig", short="3x O", label="3x Orig-Risk"),
]


BASE_CONFIGS = [
    BaseConfig(id="100k_1pct", capital=100_000, risk_pct=0.01, short="INR1L / 1%", label="INR1L Capital - 1% Daily Risk"),
    BaseConfig(id="200k_1pct", capital=200_000, risk_pct=0.01, short="INR2L / 1%", label="INR2L Capital - 1% Daily Risk"),
    BaseConfig(id="200k_10pct", capital=200_000, risk_pct=0.10, short="INR2L / 10%", label="INR2L Capital - 10% Daily Risk"),
    BaseConfig(id="500k_1pct", capital=500_000, risk_pct=0.01, short="INR5L / 1%", label="INR5L Capital - 1% Daily Risk"),
    BaseConfig(id="500k_5pct", capital=500_000, risk_pct=0.05, short="INR5L / 5%", label="INR5L Capital - 5% Daily Risk"),
    BaseConfig(id="500k_10pct", capital=500_000, risk_pct=0.10, short="INR5L / 10%", label="INR5L Capital - 10% Daily Risk"),
    BaseConfig(id="1000k_1pct", capital=1_000_000, risk_pct=0.01, short="INR10L / 1%", label="INR10L Capital - 1% Daily Risk"),
]


S1_PLAN = PrecomputedPlan(
    strategy_id="s1",
    variant_id="v1",
    timeframe="15m",
    summary_flat_path=S1_FLAT_SUMMARY_PATH,
    summary_compound_path=S1_COMPOUND_SUMMARY_PATH,
    data_flat_dir=S1_FLAT_DATA_DIR,
    data_compound_dir=S1_COMPOUND_DATA_DIR,
)

S2_PLAN = PrecomputedPlan(
    strategy_id="s2",
    variant_id="v8",
    timeframe="5m",
    summary_flat_path=S2_FLAT_SUMMARY_PATH,
    summary_compound_path=S2_COMPOUND_SUMMARY_PATH,
    data_flat_dir=S2_FLAT_DATA_DIR,
    data_compound_dir=S2_COMPOUND_DATA_DIR,
)


EXIT_BY_ID = {item.id: item for item in EXIT_SPECS}


SUMMARY_CORE_KEYS = [
    "totalTrades",
    "wins",
    "losses",
    "totalPnl",
    "totalGrossPnl",
    "totalCharges",
    "winRate",
    "avgWin",
    "avgLoss",
    "largestWin",
    "largestLoss",
    "profitFactor",
    "maxDrawdown",
    "maxConsecWins",
    "maxConsecLosses",
    "initialCapital",
    "finalCapital",
    "roc",
    "avgQty",
    "minQty",
    "maxQty",
]


S3_EXIT_RULES = {
    "e1": {"tp1_share": 0.50, "tp2_multiple": 2.0, "move_stop_to_entry": True},
    "e4": {"tp1_share": 0.70, "tp2_multiple": None, "move_stop_to_entry": False},
    "e8": {"tp1_share": 0.30, "tp2_multiple": None, "move_stop_to_entry": False},
}


def round2(value: float) -> float:
    return round(float(value), 2)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except Exception:
        return int(default)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=True), encoding="utf-8")


def build_full_configs() -> list[FullConfig]:
    configs: list[FullConfig] = []
    for base in BASE_CONFIGS:
        for lev in LEVERAGE_SPECS:
            risk_pct = base.risk_pct
            if lev.mode == "orig" and lev.lev > 0:
                risk_pct = base.risk_pct / lev.lev

            cfg_id = f"{base.id}_{lev.id}"
            short = f"{base.short} | {lev.short}"
            label = f"{base.label} | {lev.label}"
            configs.append(
                FullConfig(
                    id=cfg_id,
                    short=short,
                    label=label,
                    base_id=base.id,
                    base_capital=base.capital,
                    base_risk_pct=base.risk_pct,
                    capital=base.capital,
                    risk_pct=risk_pct,
                    lev_id=lev.id,
                    lev_mode=lev.mode,
                    lev=lev.lev,
                )
            )
    return configs


def choose_worker_count(job_count: int) -> int:
    if job_count <= 1:
        return 1

    configured = os.environ.get("COMBINED_BUILD_WORKERS")
    if configured:
        try:
            return max(1, min(job_count, int(configured)))
        except ValueError:
            pass

    return max(1, min(job_count, cpu_count() - 3))


def trade_weekday(date_value: str) -> str:
    try:
        return datetime.strptime(date_value, "%Y-%m-%d").strftime("%a")
    except Exception:
        return ""


def summarize_trades(trades: list[dict[str, Any]], initial_capital: float) -> dict[str, Any]:
    ordered = sorted(trades, key=lambda row: (str(row.get("date", "")), str(row.get("entryTime", "")), str(row.get("symbol", ""))))

    wins = [row for row in ordered if as_float(row.get("pnl")) > 0]
    losses = [row for row in ordered if as_float(row.get("pnl")) <= 0]

    total_pnl = round(sum(as_float(row.get("pnl")) for row in ordered), 2)
    total_gross = round(sum(as_float(row.get("grossPnl")) for row in ordered), 2)
    total_charges = round(sum(as_float(row.get("charges")) for row in ordered), 2)

    equity = [round(initial_capital, 2)]
    drawdown: list[float] = [0.0]
    peak = initial_capital

    max_consec_wins = 0
    max_consec_losses = 0
    consec_wins = 0
    consec_losses = 0

    for row in ordered:
        next_equity = equity[-1] + as_float(row.get("pnl"))
        equity.append(round(next_equity, 2))
        peak = max(peak, next_equity)
        dd = 0.0 if peak <= 0 else ((peak - next_equity) / peak * 100.0)
        drawdown.append(round(dd, 2))

        if as_float(row.get("pnl")) > 0:
            consec_wins += 1
            consec_losses = 0
        else:
            consec_losses += 1
            consec_wins = 0
        max_consec_wins = max(max_consec_wins, consec_wins)
        max_consec_losses = max(max_consec_losses, consec_losses)

    gross_profit = sum(as_float(row.get("grossPnl")) for row in wins)
    gross_loss = abs(sum(as_float(row.get("grossPnl")) for row in losses))

    monthly_map: dict[str, float] = defaultdict(float)
    day_group: dict[str, dict[str, Any]] = {}
    month_charges: dict[str, dict[str, float]] = defaultdict(lambda: {"trades": 0, "gross": 0.0, "charges": 0.0, "pnl": 0.0})
    exit_reasons: dict[str, int] = defaultdict(int)
    weekday_stats: dict[str, dict[str, float]] = {
        "Mon": {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0},
        "Tue": {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0},
        "Wed": {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0},
        "Thu": {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0},
        "Fri": {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0},
    }

    for row in ordered:
        date_value = str(row.get("date", ""))
        month_key = date_value[:7]
        monthly_map[month_key] += as_float(row.get("pnl"))

        if date_value not in day_group:
            day_group[date_value] = {
                "date": date_value,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl": 0.0,
                "gross": 0.0,
                "charges": 0.0,
                "symbols": set(),
            }
        day_entry = day_group[date_value]
        day_entry["trades"] += 1
        day_entry["wins"] += 1 if as_float(row.get("pnl")) > 0 else 0
        day_entry["losses"] += 1 if as_float(row.get("pnl")) <= 0 else 0
        day_entry["pnl"] += as_float(row.get("pnl"))
        day_entry["gross"] += as_float(row.get("grossPnl"))
        day_entry["charges"] += as_float(row.get("charges"))
        day_entry["symbols"].add(str(row.get("symbol", "")))

        month_charge = month_charges[month_key]
        month_charge["trades"] += 1
        month_charge["gross"] += as_float(row.get("grossPnl"))
        month_charge["charges"] += as_float(row.get("charges"))
        month_charge["pnl"] += as_float(row.get("pnl"))

        exit_reasons[str(row.get("exitReason", ""))] += 1

        weekday = trade_weekday(date_value)
        if weekday in weekday_stats:
            weekday_stats[weekday]["trades"] += 1
            weekday_stats[weekday]["wins"] += 1 if as_float(row.get("pnl")) > 0 else 0
            weekday_stats[weekday]["losses"] += 1 if as_float(row.get("pnl")) <= 0 else 0
            weekday_stats[weekday]["pnl"] += as_float(row.get("pnl"))

    day_stats = []
    for day in sorted(day_group.keys(), reverse=True):
        row = day_group[day]
        day_stats.append(
            {
                "date": row["date"],
                "trades": int(row["trades"]),
                "wins": int(row["wins"]),
                "losses": int(row["losses"]),
                "pnl": round(row["pnl"], 2),
                "gross": round(row["gross"], 2),
                "charges": round(row["charges"], 2),
                "symbols": ", ".join(sorted(row["symbols"])),
            }
        )

    monthly_charges = {
        month: {
            "trades": int(values["trades"]),
            "gross": round(values["gross"], 2),
            "charges": round(values["charges"], 2),
            "pnl": round(values["pnl"], 2),
        }
        for month, values in sorted(month_charges.items())
    }

    weekly_stats: dict[str, float] = defaultdict(float)
    for row in ordered:
        date_value = str(row.get("date", ""))
        try:
            dt = datetime.strptime(date_value, "%Y-%m-%d")
        except Exception:
            continue
        week_key = f"{dt.isocalendar().year}-W{dt.isocalendar().week:02d}"
        weekly_stats[week_key] += as_float(row.get("pnl"))

    total_trades = len(ordered)
    avg_qty = round(sum(as_float(row.get("qty")) for row in ordered) / total_trades, 2) if total_trades else 0.0

    return {
        "totalTrades": total_trades,
        "wins": len(wins),
        "losses": len(losses),
        "totalPnl": total_pnl,
        "totalGrossPnl": total_gross,
        "totalCharges": total_charges,
        "winRate": round((len(wins) / total_trades * 100.0) if total_trades else 0.0, 2),
        "avgWin": round(sum(as_float(row.get("pnl")) for row in wins) / len(wins), 2) if wins else 0.0,
        "avgLoss": round(sum(as_float(row.get("pnl")) for row in losses) / len(losses), 2) if losses else 0.0,
        "largestWin": round(max((as_float(row.get("pnl")) for row in ordered), default=0.0), 2),
        "largestLoss": round(min((as_float(row.get("pnl")) for row in ordered), default=0.0), 2),
        "profitFactor": round((gross_profit / gross_loss), 2) if gross_loss else 0.0,
        "maxDrawdown": round(max(drawdown) if drawdown else 0.0, 2),
        "maxConsecWins": max_consec_wins,
        "maxConsecLosses": max_consec_losses,
        "initialCapital": round(initial_capital, 2),
        "finalCapital": round(initial_capital + total_pnl, 2),
        "roc": round((total_pnl / initial_capital * 100.0) if initial_capital else 0.0, 2),
        "avgQty": avg_qty,
        "minQty": min((as_int(row.get("qty")) for row in ordered), default=0),
        "maxQty": max((as_int(row.get("qty")) for row in ordered), default=0),
        "equity": equity,
        "drawdown": drawdown,
        "monthly": {month: round(value, 2) for month, value in sorted(monthly_map.items())},
        "weekly": {week: round(value, 2) for week, value in sorted(weekly_stats.items())},
        "weekdayStats": {
            day: {
                "trades": int(data["trades"]),
                "wins": int(data["wins"]),
                "losses": int(data["losses"]),
                "pnl": round(data["pnl"], 2),
            }
            for day, data in weekday_stats.items()
        },
        "dayStats": day_stats,
        "monthlyCharges": monthly_charges,
        "exitReasons": dict(sorted(exit_reasons.items())),
        "trades": ordered,
    }


def core_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {key: summary.get(key) for key in SUMMARY_CORE_KEYS}


def apply_trade_log_limit(detail: dict[str, Any]) -> None:
    trades = detail.get("trades")
    if not isinstance(trades, list):
        return
    if len(trades) <= TRADE_LOG_LIMIT:
        return
    detail["trades"] = trades[-TRADE_LOG_LIMIT:]
    detail["tradeLogLimitedTo"] = TRADE_LOG_LIMIT


def add_combo_metadata(detail: dict[str, Any], strategy_meta: StrategyMeta, mode: str, exit_spec: ExitSpec, cfg: FullConfig) -> None:
    detail["strategyId"] = strategy_meta.id
    detail["strategyTitle"] = strategy_meta.title
    detail["strategySubtitle"] = strategy_meta.subtitle
    detail["sourceEngine"] = strategy_meta.source_engine
    detail["mode"] = mode
    detail["exitId"] = exit_spec.id
    detail["exitName"] = exit_spec.name
    detail["configId"] = cfg.id
    detail["configLabel"] = cfg.label
    detail["configShort"] = cfg.short
    detail["baseConfigId"] = cfg.base_id
    detail["levId"] = cfg.lev_id
    detail["levMode"] = cfg.lev_mode
    detail["leverage"] = float(cfg.lev)
    detail["riskPct"] = float(cfg.risk_pct)


def source_combo_key(plan: PrecomputedPlan, exit_id: str, cfg_id: str) -> str:
    return f"{plan.variant_id}_{exit_id}_{plan.timeframe}_{cfg_id}"


def strategy1_source_combo_key(exit_id: str, cfg_id: str) -> str:
    return f"{S1_4V1_VARIANT_ID}_{exit_id}_15m_{cfg_id}"


def read_first_existing_text(paths: tuple[Path, ...]) -> tuple[Path, str]:
    for path in paths:
        if path.exists():
            return path, path.read_text(encoding="utf-8")
    joined = ", ".join(str(path) for path in paths)
    raise FileNotFoundError(f"None of the expected files exist: {joined}")


def extract_json_array_after_marker(text: str, marker: str) -> str:
    start = text.find(marker)
    if start == -1:
        raise ValueError(f"Could not find marker {marker!r}")

    array_start = text.find("[", start)
    if array_start == -1:
        raise ValueError(f"Could not find JSON array after marker {marker!r}")

    depth = 0
    in_string = False
    escape_next = False
    for index in range(array_start, len(text)):
        char = text[index]
        if in_string:
            if escape_next:
                escape_next = False
            elif char == "\\":
                escape_next = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return text[array_start : index + 1]

    raise ValueError(f"Unterminated JSON array after marker {marker!r}")


def normalize_legacy_dashboard_trade(row: dict[str, Any], entry_clock: str) -> dict[str, Any]:
    trade_date = str(row.get("date", ""))
    entry_price = round2(as_float(row.get("entry")))
    stop_price = round2(as_float(row.get("sl")))
    tp1_price = round2(as_float(row.get("tp1")))
    exit_price = round2(as_float(row.get("exit")))
    qty = max(1, as_int(row.get("qty"), 1))
    exit_reason = str(row.get("reason", ""))
    direction = infer_direction(None, entry_price, tp1_price)

    if exit_reason == "EOD Close":
        exit_time = f"{trade_date} 15:15:00"
    else:
        exit_time = f"{trade_date} {entry_clock}"

    return {
        "symbol": str(row.get("symbol", "")),
        "date": trade_date,
        "direction": direction,
        "entryPrice": entry_price,
        "slPrice": stop_price,
        "tp1Price": tp1_price,
        "tp2Price": tp1_price,
        "riskPerSh": round2(abs(entry_price - stop_price)),
        "qty": qty,
        "capitalDeployed": round2(entry_price * qty),
        "riskAlloc": round2(as_float(row.get("risk"))),
        "stocksInDay": max(1, as_int(row.get("stk"), 1)),
        "entryTime": f"{trade_date} {entry_clock}",
        "exitPrice": exit_price,
        "exitTime": exit_time,
        "exitReason": exit_reason,
        "tp1Hit": exit_reason in {"CTC Exit", "TP2 Hit", "SL(R)"},
        "grossPnl": round2(as_float(row.get("gross_pnl"))),
        "charges": round2(as_float(row.get("charges"))),
        "pnl": round2(as_float(row.get("net_pnl"))),
    }


def load_legacy_dashboard_detail(trade_index: int, entry_clock: str, source_variant_id: str) -> dict[str, Any]:
    dashboard_path, dashboard_text = read_first_existing_text(S1_LEGACY_DASHBOARD_PATHS)
    raw_payload = extract_json_array_after_marker(dashboard_text, f"initTrades({trade_index},")
    raw_trades = json.loads(raw_payload)
    if not isinstance(raw_trades, list) or not raw_trades:
        raise ValueError(f"Invalid legacy strategy payload in {dashboard_path}")

    trades = [normalize_legacy_dashboard_trade(row, entry_clock) for row in raw_trades if isinstance(row, dict)]
    if not trades:
        raise ValueError(f"Legacy strategy payload in {dashboard_path} did not contain any trades")

    detail = summarize_trades(trades, 500_000.0)
    detail["legacySource"] = str(dashboard_path)
    detail["legacyVariantId"] = source_variant_id
    return detail


def infer_legacy_final_leg_price(trade: dict[str, Any]) -> float:
    exit_reason = str(trade.get("exitReason", ""))
    exit_price = as_float(trade.get("exitPrice"))
    if exit_reason == "SL Hit":
        return exit_price

    tp1 = as_float(trade.get("tp1Price"))
    return (exit_price - LEGACY_ORIGINAL_TP1_SHARE * tp1) / (1.0 - LEGACY_ORIGINAL_TP1_SHARE)


def legacy_variant_exit(trade: dict[str, Any], exit_id: str) -> tuple[float, str, bool]:
    entry = as_float(trade.get("entryPrice"))
    stop = as_float(trade.get("slPrice"))
    tp1 = as_float(trade.get("tp1Price"))
    direction = infer_direction(trade.get("direction"), entry, tp1)
    reason = str(trade.get("exitReason", ""))

    if exit_id == "e8":
        return as_float(trade.get("exitPrice")), reason, reason != "SL Hit"

    if reason == "SL Hit":
        return stop, "SL Hit", False

    final_leg = infer_legacy_final_leg_price(trade)
    if exit_id == "e4":
        exit_price = (0.70 * tp1) + (0.30 * final_leg)
        return round2(exit_price), reason, True

    risk_per_share = abs(entry - stop)
    tp2 = entry + (2.0 * risk_per_share if direction == "LONG" else -2.0 * risk_per_share)
    if reason == "SL(R)":
        return round2((0.50 * tp1) + (0.50 * entry)), "CTC Exit", True

    reached_tp2_by_close = final_leg >= tp2 if direction == "LONG" else final_leg <= tp2
    remainder_exit = tp2 if reached_tp2_by_close else final_leg
    exit_reason = "TP2 Hit" if reached_tp2_by_close else "EOD Close"
    return round2((0.50 * tp1) + (0.50 * remainder_exit)), exit_reason, True


def rebuild_legacy_detail_from_original(source_detail: dict[str, Any], exit_id: str, cfg: FullConfig, compound: bool) -> dict[str, Any]:
    source_trades = source_detail.get("trades")
    if not isinstance(source_trades, list):
        raise ValueError("Legacy source detail is missing trades")

    trades_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in sorted(
        source_trades,
        key=lambda row: (str(row.get("date", "")), str(row.get("entryTime", "")), str(row.get("symbol", ""))),
    ):
        trade_date = str(trade.get("date", ""))
        if trade_date:
            trades_by_date[trade_date].append(trade)

    rebuilt_trades: list[dict[str, Any]] = []
    current_equity = float(cfg.capital)

    for trade_date in sorted(trades_by_date.keys()):
        day_trades = trades_by_date[trade_date]
        day_capital = current_equity
        day_risk_budget = day_capital * float(cfg.risk_pct)

        raw_qtys: list[int] = []
        requested_capital = 0.0
        for trade in day_trades:
            split_count = max(1, as_int(trade.get("stocksInDay"), len(day_trades)))
            risk_per_trade = day_risk_budget / split_count
            risk_per_share = as_float(trade.get("riskPerSh"))
            if risk_per_share <= 0:
                risk_per_share = abs(as_float(trade.get("entryPrice")) - as_float(trade.get("slPrice")))
            raw_qty = max(1, int(risk_per_trade / risk_per_share)) if risk_per_share > 0 else 1
            raw_qtys.append(raw_qty)
            requested_capital += raw_qty * as_float(trade.get("entryPrice"))

        leverage_cap = day_capital * float(cfg.lev)
        scale = 1.0
        if requested_capital > leverage_cap and requested_capital > 0:
            scale = leverage_cap / requested_capital

        day_pnl = 0.0
        for trade, raw_qty in zip(day_trades, raw_qtys):
            final_qty = max(1, int(raw_qty * scale))
            entry = as_float(trade.get("entryPrice"))
            exit_price, exit_reason, tp1_hit = legacy_variant_exit(trade, exit_id)
            direction = infer_direction(trade.get("direction"), entry, as_float(trade.get("tp1Price")))
            gross_pnl = (exit_price - entry) * final_qty if direction == "LONG" else (entry - exit_price) * final_qty
            charges = calculate_charges(entry, exit_price, final_qty, direction)
            rebuilt_trade = dict(trade)
            rebuilt_trade["qty"] = final_qty
            rebuilt_trade["capitalDeployed"] = round2(entry * final_qty)
            rebuilt_trade["riskAlloc"] = round2(as_float(trade.get("riskPerSh")) * final_qty)
            rebuilt_trade["tp2Price"] = round2(entry + (2.0 * abs(entry - as_float(trade.get("slPrice"))) if direction == "LONG" else -2.0 * abs(entry - as_float(trade.get("slPrice")))))
            rebuilt_trade["exitPrice"] = round2(exit_price)
            rebuilt_trade["exitReason"] = exit_reason
            rebuilt_trade["tp1Hit"] = tp1_hit
            rebuilt_trade["grossPnl"] = round2(gross_pnl)
            rebuilt_trade["charges"] = round2(charges)
            rebuilt_trade["pnl"] = round2(gross_pnl - charges)
            rebuilt_trades.append(rebuilt_trade)
            day_pnl += as_float(rebuilt_trade.get("pnl"))

        if compound:
            current_equity = max(float(cfg.capital) * 0.10, current_equity + day_pnl)

    return summarize_trades(rebuilt_trades, float(cfg.capital))


def scale_trade_to_qty(source_trade: dict[str, Any], qty: int, risk_alloc: float) -> dict[str, Any]:
    scaled = dict(source_trade)
    old_qty = max(1, as_int(source_trade.get("qty"), 1))
    factor = float(qty) / float(old_qty)

    scaled["qty"] = qty
    scaled["riskAlloc"] = round2(risk_alloc)
    scaled["capitalDeployed"] = round2(as_float(source_trade.get("entryPrice")) * qty)
    scaled["grossPnl"] = round2(as_float(source_trade.get("grossPnl")) * factor)
    scaled["charges"] = round2(as_float(source_trade.get("charges")) * factor)
    scaled["pnl"] = round2(as_float(scaled.get("grossPnl")) - as_float(scaled.get("charges")))
    return scaled


def rebuild_compound_detail_from_flat(source_detail: dict[str, Any], cfg: FullConfig) -> dict[str, Any]:
    source_trades = source_detail.get("trades")
    if not isinstance(source_trades, list):
        raise ValueError("Strategy 1 source detail is missing trades")

    trades_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in sorted(
        source_trades,
        key=lambda row: (str(row.get("date", "")), str(row.get("entryTime", "")), str(row.get("symbol", ""))),
    ):
        trade_date = str(trade.get("date", ""))
        if trade_date:
            trades_by_date[trade_date].append(trade)

    rebuilt_trades: list[dict[str, Any]] = []
    current_equity = float(cfg.capital)

    for trade_date in sorted(trades_by_date.keys()):
        day_trades = trades_by_date[trade_date]
        if not day_trades:
            continue

        day_risk_budget = current_equity * float(cfg.risk_pct)
        risk_per_trade = day_risk_budget / len(day_trades)

        raw_qtys: list[int] = []
        requested_capital = 0.0
        for trade in day_trades:
            risk_per_share = as_float(trade.get("riskPerSh"))
            if risk_per_share <= 0:
                risk_per_share = abs(as_float(trade.get("entryPrice")) - as_float(trade.get("slPrice")))
            raw_qty = max(1, int(risk_per_trade / risk_per_share)) if risk_per_share > 0 else 1
            raw_qtys.append(raw_qty)
            requested_capital += raw_qty * as_float(trade.get("entryPrice"))

        leverage_cap = current_equity * float(cfg.lev)
        scale = 1.0
        if requested_capital > leverage_cap and requested_capital > 0:
            scale = leverage_cap / requested_capital

        day_pnl = 0.0
        for trade, raw_qty in zip(day_trades, raw_qtys):
            final_qty = max(1, int(raw_qty * scale))
            rebuilt_trade = scale_trade_to_qty(trade, final_qty, risk_per_trade)
            rebuilt_trades.append(rebuilt_trade)
            day_pnl += as_float(rebuilt_trade.get("pnl"))

        current_equity = max(float(cfg.capital) * 0.10, current_equity + day_pnl)

    return summarize_trades(rebuilt_trades, float(cfg.capital))


def generate_legacy_dashboard_strategy(
    strategy_meta: StrategyMeta,
    full_configs: list[FullConfig],
    trade_index: int,
    entry_clock: str,
    source_variant_id: str,
) -> None:
    strategy_dir = OUT_DIR / strategy_meta.id
    data_flat_dir = strategy_dir / "data_flat"
    data_compound_dir = strategy_dir / "data_compound"
    data_flat_dir.mkdir(parents=True, exist_ok=True)
    data_compound_dir.mkdir(parents=True, exist_ok=True)

    source_detail = load_legacy_dashboard_detail(trade_index, entry_clock, source_variant_id)

    summary_flat: dict[str, Any] = {}
    summary_compound: dict[str, Any] = {}
    total = len(EXIT_SPECS) * len(full_configs) * 2
    done = 0

    for exit_spec in EXIT_SPECS:
        for cfg in full_configs:
            out_key = f"{strategy_meta.id}_{exit_spec.id}_{cfg.id}"

            if exit_spec.id == "e8" and cfg.id == "500k_5pct_3xS":
                flat_detail = json.loads(json.dumps(source_detail))
            else:
                flat_detail = rebuild_legacy_detail_from_original(source_detail, exit_spec.id, cfg, compound=False)
            compound_detail = rebuild_legacy_detail_from_original(source_detail, exit_spec.id, cfg, compound=True)

            apply_trade_log_limit(flat_detail)
            add_combo_metadata(flat_detail, strategy_meta, "flat", exit_spec, cfg)
            write_json(data_flat_dir / f"{out_key}.json", flat_detail)
            summary_flat[out_key] = core_summary(flat_detail)
            done += 1
            if done % 30 == 0 or done == total:
                print(f"[{strategy_meta.id}] {done}/{total} combos done")

            apply_trade_log_limit(compound_detail)
            add_combo_metadata(compound_detail, strategy_meta, "compound", exit_spec, cfg)
            write_json(data_compound_dir / f"{out_key}.json", compound_detail)
            summary_compound[out_key] = core_summary(compound_detail)
            done += 1
            if done % 30 == 0 or done == total:
                print(f"[{strategy_meta.id}] {done}/{total} combos done")

    write_json(strategy_dir / "summary_flat.json", dict(sorted(summary_flat.items())))
    write_json(strategy_dir / "summary_compound.json", dict(sorted(summary_compound.items())))


def validate_precomputed_plan(plan: PrecomputedPlan) -> None:
    required = [
        plan.summary_flat_path,
        plan.summary_compound_path,
        plan.data_flat_dir,
        plan.data_compound_dir,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"Missing precomputed source path: {path}")


def generate_precomputed_strategy(plan: PrecomputedPlan, strategy_meta: StrategyMeta, full_configs: list[FullConfig]) -> None:
    validate_precomputed_plan(plan)

    strategy_dir = OUT_DIR / strategy_meta.id
    data_flat_dir = strategy_dir / "data_flat"
    data_compound_dir = strategy_dir / "data_compound"
    data_flat_dir.mkdir(parents=True, exist_ok=True)
    data_compound_dir.mkdir(parents=True, exist_ok=True)

    src_summary_flat = load_json(plan.summary_flat_path)
    src_summary_compound = load_json(plan.summary_compound_path)
    if not isinstance(src_summary_flat, dict) or not isinstance(src_summary_compound, dict):
        raise ValueError(f"Invalid summary source JSON for {strategy_meta.id}")

    summary_flat: dict[str, Any] = {}
    summary_compound: dict[str, Any] = {}

    total = len(EXIT_SPECS) * len(full_configs) * 2
    done = 0

    for mode in ("flat", "compound"):
        mode_dir = data_flat_dir if mode == "flat" else data_compound_dir
        source_summary = src_summary_flat if mode == "flat" else src_summary_compound
        source_data_dir = plan.data_flat_dir if mode == "flat" else plan.data_compound_dir

        for exit_spec in EXIT_SPECS:
            for cfg in full_configs:
                source_key = source_combo_key(plan, exit_spec.id, cfg.id)
                source_detail_path = source_data_dir / f"{source_key}.json"
                source_core = source_summary.get(source_key)

                if not isinstance(source_core, dict):
                    raise KeyError(f"Missing source summary key {source_key} for strategy {strategy_meta.id} mode={mode}")
                if not source_detail_path.exists():
                    raise FileNotFoundError(f"Missing source detail file: {source_detail_path}")

                detail = load_json(source_detail_path)
                if not isinstance(detail, dict):
                    raise ValueError(f"Invalid detail payload in {source_detail_path}")

                apply_trade_log_limit(detail)
                add_combo_metadata(detail, strategy_meta, mode, exit_spec, cfg)

                out_key = f"{strategy_meta.id}_{exit_spec.id}_{cfg.id}"
                write_json(mode_dir / f"{out_key}.json", detail)

                if mode == "flat":
                    summary_flat[out_key] = core_summary(source_core)
                else:
                    summary_compound[out_key] = core_summary(source_core)

                done += 1
                if done % 30 == 0 or done == total:
                    print(f"[{strategy_meta.id}] {done}/{total} combos done")

    write_json(strategy_dir / "summary_flat.json", dict(sorted(summary_flat.items())))
    write_json(strategy_dir / "summary_compound.json", dict(sorted(summary_compound.items())))


def load_strategy3_engine_module() -> Any:
    if not STRATEGY3_ENGINE_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {STRATEGY3_ENGINE_PATH}")

    spec = importlib.util.spec_from_file_location("search_strategy3_fixedrisk", STRATEGY3_ENGINE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load strategy3 engine module from {STRATEGY3_ENGINE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def parse_single_combo(combo: str, score_filter_ids: list[str]) -> tuple[str, str] | None:
    if not combo.startswith("single_"):
        return None

    body = combo[len("single_") :]
    for score_id in sorted(score_filter_ids, key=len, reverse=True):
        suffix = f"_{score_id}"
        if body.endswith(suffix):
            variant_id = body[: -len(suffix)]
            if variant_id:
                return variant_id, score_id
    if body:
        return body, "all"
    return None


def parse_strategy3_max_trades(config_id: str) -> int:
    match = re.search(r"_t(\d+)$", config_id)
    if not match:
        return 1
    return max(1, int(match.group(1)))


def select_strategy3_setups(engine_module: Any, raw_candidates: dict[str, list[dict[str, Any]]], payload_summary: dict[str, Any]) -> list[dict[str, Any]]:
    combo = str(payload_summary.get("strategyCombo") or "")
    raw_strategy_id = str(payload_summary.get("rawStrategyId") or "")

    score_filters = list(getattr(engine_module, "SCORE_FILTERS", []))
    score_filter_ids = [str(item.id) for item in score_filters if hasattr(item, "id")]

    parsed_single = parse_single_combo(combo, score_filter_ids)
    if parsed_single:
        variant_id, score_filter_id = parsed_single
        base = list(raw_candidates.get(variant_id, []))
        if not base:
            raise ValueError(f"Strategy3 payload references unknown variant: {variant_id}")

        score_filter_obj = next((item for item in score_filters if getattr(item, "id", "") == score_filter_id), None)
        if score_filter_obj is None:
            return base

        filtered, _ = engine_module.apply_score_filter(base, score_filter_obj)
        return list(filtered)

    if "+" in combo:
        slices_data, _ = engine_module.build_slice_library(raw_candidates)
        merged: list[dict[str, Any]] = []
        for slice_id in [chunk.strip() for chunk in combo.split("+") if chunk.strip()]:
            setups = slices_data.get(slice_id)
            if not setups:
                raise ValueError(f"Strategy3 payload references missing slice: {slice_id}")
            merged.extend(setups)
        return merged

    if raw_strategy_id and raw_strategy_id in raw_candidates:
        return list(raw_candidates[raw_strategy_id])

    if combo and combo in raw_candidates:
        return list(raw_candidates[combo])

    raise ValueError(f"Unable to resolve Strategy3 setups for combo={combo!r}, rawStrategyId={raw_strategy_id!r}")


def load_strategy3_context() -> Strategy3Context:
    if not STRATEGY3_PAYLOAD_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {STRATEGY3_PAYLOAD_PATH}")

    payload = load_json(STRATEGY3_PAYLOAD_PATH)
    if not isinstance(payload, dict):
        raise ValueError("Invalid strategy3 payload JSON")

    payload_summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    payload_meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    engine_module = load_strategy3_engine_module()
    raw_candidates = engine_module.scan_all(use_multiprocessing=True)
    if not isinstance(raw_candidates, dict):
        raise ValueError("Strategy3 scan_all returned invalid payload")

    setups = select_strategy3_setups(engine_module, raw_candidates, payload_summary)
    if not setups:
        raise ValueError("No Strategy3 setups resolved from payload/cache")

    title = str(payload_meta.get("title") or "Strategy 3 - Fixed-Risk Mean Reversion Candlestick Composite")
    subtitle = str(
        payload_meta.get("subtitle")
        or "Built from raw 15-minute Nifty 500 data with fixed-risk controls and explicit exit variants."
    )
    config_id = str(payload_summary.get("configId") or "r5_l3_t1")
    max_trades = parse_strategy3_max_trades(config_id)

    return Strategy3Context(
        title=title,
        subtitle=subtitle,
        source_engine="temp/search_strategy3_fixedrisk.py + temp/fixedrisk_mr_candle_candidates.pkl",
        max_trades_per_day=max_trades,
        setups=tuple(setups),
    )


def calculate_charges(entry_price: float, exit_price: float, qty: int, direction: str) -> float:
    buy_price = entry_price if direction == "LONG" else exit_price
    sell_price = exit_price if direction == "LONG" else entry_price

    buy_turnover = buy_price * qty
    sell_turnover = sell_price * qty
    total_turnover = buy_turnover + sell_turnover

    brokerage = 0.0
    stt = sell_turnover * 0.00025
    txn = total_turnover * 0.0000325
    sebi = total_turnover * 0.000001
    stamp = buy_turnover * 0.00003
    gst = 0.18 * (brokerage + txn + sebi)
    return brokerage + stt + txn + sebi + stamp + gst


def normalize_clock(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    if " " in text:
        text = text.split(" ")[-1]
    return text or fallback


def infer_direction(direction_value: Any, entry: float, tp1: float) -> str:
    direction = str(direction_value or "").upper().strip()
    if direction in {"LONG", "SHORT"}:
        return direction
    return "SHORT" if tp1 < entry else "LONG"


def simulate_strategy3_trade(setup: dict[str, Any], qty: int, exit_id: str, strategy_title: str) -> dict[str, Any]:
    if exit_id not in S3_EXIT_RULES:
        raise ValueError(f"Unsupported Strategy3 exit: {exit_id}")

    rule = S3_EXIT_RULES[exit_id]

    entry = as_float(setup.get("entryPrice"))
    stop = as_float(setup.get("slPrice"))
    tp1 = as_float(setup.get("tp1Price"))
    direction = infer_direction(setup.get("direction"), entry, tp1)

    candles = setup.get("candles") if isinstance(setup.get("candles"), list) else []
    if not candles:
        raise ValueError("Strategy3 setup has no candle path")

    risk_per_share = abs(entry - stop)
    if risk_per_share <= 0:
        raise ValueError("Strategy3 setup has non-positive risk distance")

    tp2_multiple = rule["tp2_multiple"]
    if tp2_multiple is None:
        tp2 = None
    elif direction == "LONG":
        tp2 = round2(entry + risk_per_share * float(tp2_multiple))
    else:
        tp2 = round2(entry - risk_per_share * float(tp2_multiple))

    tp1_share = float(rule["tp1_share"])
    tp_qty = max(1, int(round(qty * tp1_share))) if tp1_share > 0 else 0
    tp_qty = min(tp_qty, qty)
    rem_qty = qty - tp_qty

    gross_pnl = 0.0
    total_charges = 0.0
    tp_hit = False

    last_candle = candles[-1]
    fallback_exit_price = as_float(last_candle.get("o"), entry)
    fallback_exit_time = str(last_candle.get("dt") or setup.get("entryTime") or "")
    exit_price = fallback_exit_price
    exit_time = fallback_exit_time
    exit_reason = "EOD Close"

    sim_candles = candles[:-1] if len(candles) > 1 else candles
    for candle in sim_candles:
        high = as_float(candle.get("h"))
        low = as_float(candle.get("l"))
        dt = str(candle.get("dt") or "")

        if not tp_hit:
            if direction == "LONG":
                if low <= stop:
                    gross_pnl = (stop - entry) * qty
                    total_charges = calculate_charges(entry, stop, qty, direction)
                    exit_price = stop
                    exit_time = dt
                    exit_reason = "SL Hit"
                    break
                if tp_qty > 0 and high >= tp1:
                    tp_hit = True
                    gross_pnl += (tp1 - entry) * tp_qty
                    total_charges += calculate_charges(entry, tp1, tp_qty, direction)
                    if rem_qty == 0:
                        exit_price = tp1
                        exit_time = dt
                        exit_reason = "TP1 Full"
                        break
            else:
                if high >= stop:
                    gross_pnl = (entry - stop) * qty
                    total_charges = calculate_charges(entry, stop, qty, direction)
                    exit_price = stop
                    exit_time = dt
                    exit_reason = "SL Hit"
                    break
                if tp_qty > 0 and low <= tp1:
                    tp_hit = True
                    gross_pnl += (entry - tp1) * tp_qty
                    total_charges += calculate_charges(entry, tp1, tp_qty, direction)
                    if rem_qty == 0:
                        exit_price = tp1
                        exit_time = dt
                        exit_reason = "TP1 Full"
                        break
        else:
            active_stop = entry if bool(rule["move_stop_to_entry"]) else stop
            if rem_qty <= 0:
                break

            if direction == "LONG":
                if low <= active_stop:
                    gross_pnl += (active_stop - entry) * rem_qty
                    total_charges += calculate_charges(entry, active_stop, rem_qty, direction)
                    exit_price = active_stop
                    exit_time = dt
                    exit_reason = "SL Hit (Rem)"
                    break
                if tp2 is not None and high >= tp2:
                    gross_pnl += (tp2 - entry) * rem_qty
                    total_charges += calculate_charges(entry, tp2, rem_qty, direction)
                    exit_price = tp2
                    exit_time = dt
                    exit_reason = "TP2 Hit"
                    break
            else:
                if high >= active_stop:
                    gross_pnl += (entry - active_stop) * rem_qty
                    total_charges += calculate_charges(entry, active_stop, rem_qty, direction)
                    exit_price = active_stop
                    exit_time = dt
                    exit_reason = "SL Hit (Rem)"
                    break
                if tp2 is not None and low <= tp2:
                    gross_pnl += (entry - tp2) * rem_qty
                    total_charges += calculate_charges(entry, tp2, rem_qty, direction)
                    exit_price = tp2
                    exit_time = dt
                    exit_reason = "TP2 Hit"
                    break
    else:
        eod_price = fallback_exit_price
        if tp_hit:
            if rem_qty > 0:
                if direction == "LONG":
                    gross_pnl += (eod_price - entry) * rem_qty
                else:
                    gross_pnl += (entry - eod_price) * rem_qty
                total_charges += calculate_charges(entry, eod_price, rem_qty, direction)
        else:
            if direction == "LONG":
                gross_pnl = (eod_price - entry) * qty
            else:
                gross_pnl = (entry - eod_price) * qty
            total_charges = calculate_charges(entry, eod_price, qty, direction)

        exit_price = eod_price
        exit_time = fallback_exit_time
        exit_reason = "EOD Close"

    pnl = gross_pnl - total_charges

    return {
        "date": str(setup.get("date", "")),
        "entryTime": normalize_clock(setup.get("entryTime"), "10:00:00"),
        "exitTime": normalize_clock(exit_time, "15:15:00"),
        "symbol": str(setup.get("symbol", "")),
        "direction": direction,
        "strategyId": "s3",
        "strategyTitle": strategy_title,
        "note": str(setup.get("note", "")),
        "entryPrice": round2(entry),
        "exitPrice": round2(exit_price),
        "slPrice": round2(stop),
        "tp1Price": round2(tp1),
        "qty": int(qty),
        "grossPnl": round2(gross_pnl),
        "charges": round2(total_charges),
        "pnl": round2(pnl),
        "exitReason": exit_reason,
        "score": round(as_float(setup.get("score")), 6),
    }


def run_strategy3_backtest(
    setups: tuple[dict[str, Any], ...],
    cfg: FullConfig,
    exit_id: str,
    compound: bool,
    max_trades_per_day: int,
    strategy_title: str,
) -> dict[str, Any]:
    setups_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for setup in setups:
        date_value = str(setup.get("date", ""))
        if not date_value:
            continue
        setups_by_date[date_value].append(setup)

    trades: list[dict[str, Any]] = []
    current_equity = float(cfg.capital)
    max_trades = max(1, int(max_trades_per_day))

    for trade_date in sorted(setups_by_date.keys()):
        day_setups = setups_by_date[trade_date]
        candidates = sorted(day_setups, key=lambda row: as_float(row.get("score")), reverse=True)[:max_trades]
        if not candidates:
            continue

        day_capital = current_equity if compound else float(cfg.capital)
        day_risk_budget = max(0.0, day_capital * float(cfg.risk_pct))
        risk_per_trade = day_risk_budget / len(candidates) if candidates else 0.0

        raw_qtys: list[int] = []
        requested_capital = 0.0
        for setup in candidates:
            entry = as_float(setup.get("entryPrice"))
            stop = as_float(setup.get("slPrice"))
            risk_per_share = abs(entry - stop)
            qty = max(1, int(risk_per_trade / risk_per_share)) if risk_per_share > 0 else 0
            raw_qtys.append(qty)
            requested_capital += qty * entry

        leverage_cap = day_capital * float(cfg.lev)
        scale = 1.0
        if requested_capital > leverage_cap and requested_capital > 0:
            scale = leverage_cap / requested_capital

        day_pnl = 0.0
        for setup, raw_qty in zip(candidates, raw_qtys):
            qty = max(1, int(raw_qty * scale))
            if qty <= 0:
                continue
            trade = simulate_strategy3_trade(setup, qty, exit_id, strategy_title)
            trades.append(trade)
            day_pnl += as_float(trade.get("pnl"))

        if compound:
            current_equity = max(float(cfg.capital) * 0.10, current_equity + day_pnl)

    return summarize_trades(trades, float(cfg.capital))


_S3_MP_SETUPS: tuple[dict[str, Any], ...] = ()
_S3_MP_MAX_TRADES: int = 1
_S3_MP_STRATEGY_TITLE: str = "Strategy 3"


def init_strategy3_pool(setups: tuple[dict[str, Any], ...], max_trades_per_day: int, strategy_title: str) -> None:
    global _S3_MP_SETUPS, _S3_MP_MAX_TRADES, _S3_MP_STRATEGY_TITLE
    _S3_MP_SETUPS = setups
    _S3_MP_MAX_TRADES = max_trades_per_day
    _S3_MP_STRATEGY_TITLE = strategy_title


def run_strategy3_combo_worker(task: tuple[str, str, dict[str, Any]]) -> tuple[str, str, str, dict[str, Any]]:
    mode, exit_id, cfg_payload = task
    cfg = FullConfig(**cfg_payload)
    detail = run_strategy3_backtest(
        setups=_S3_MP_SETUPS,
        cfg=cfg,
        exit_id=exit_id,
        compound=(mode == "compound"),
        max_trades_per_day=_S3_MP_MAX_TRADES,
        strategy_title=_S3_MP_STRATEGY_TITLE,
    )
    return mode, exit_id, cfg.id, detail


def generate_strategy3_data(strategy_meta: StrategyMeta, context: Strategy3Context, full_configs: list[FullConfig]) -> None:
    strategy_dir = OUT_DIR / strategy_meta.id
    data_flat_dir = strategy_dir / "data_flat"
    data_compound_dir = strategy_dir / "data_compound"
    data_flat_dir.mkdir(parents=True, exist_ok=True)
    data_compound_dir.mkdir(parents=True, exist_ok=True)

    cfg_lookup = {cfg.id: cfg for cfg in full_configs}
    summary_flat: dict[str, Any] = {}
    summary_compound: dict[str, Any] = {}

    tasks: list[tuple[str, str, dict[str, Any]]] = []
    for mode in ("flat", "compound"):
        for exit_spec in EXIT_SPECS:
            for cfg in full_configs:
                tasks.append((mode, exit_spec.id, asdict(cfg)))

    workers = choose_worker_count(len(tasks))
    print(f"[s3] running {len(tasks)} combinations with {workers} worker(s)")

    done = 0
    total = len(tasks)
    with Pool(
        processes=workers,
        initializer=init_strategy3_pool,
        initargs=(context.setups, context.max_trades_per_day, context.title),
    ) as pool:
        for mode, exit_id, cfg_id, detail in pool.imap_unordered(run_strategy3_combo_worker, tasks, chunksize=1):
            cfg = cfg_lookup[cfg_id]
            exit_spec = EXIT_BY_ID[exit_id]

            apply_trade_log_limit(detail)
            add_combo_metadata(detail, strategy_meta, mode, exit_spec, cfg)

            key = f"{strategy_meta.id}_{exit_id}_{cfg_id}"
            mode_dir = data_flat_dir if mode == "flat" else data_compound_dir
            write_json(mode_dir / f"{key}.json", detail)

            if mode == "flat":
                summary_flat[key] = core_summary(detail)
            else:
                summary_compound[key] = core_summary(detail)

            done += 1
            if done % 30 == 0 or done == total:
                print(f"[s3] {done}/{total} combos done")

    write_json(strategy_dir / "summary_flat.json", dict(sorted(summary_flat.items())))
    write_json(strategy_dir / "summary_compound.json", dict(sorted(summary_compound.items())))


def write_meta(strategies: list[StrategyMeta], full_configs: list[FullConfig]) -> None:
    meta = {
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "modes": [
            {"id": "flat", "label": "Flat Capital"},
            {"id": "compound", "label": "Compounding"},
        ],
        "strategies": [
            {
                "id": strategy.id,
                "title": strategy.title,
                "subtitle": strategy.subtitle,
                "sourceEngine": strategy.source_engine,
                "maxTrades": strategy.max_trades,
            }
            for strategy in strategies
        ],
        "exits": [asdict(exit_spec) for exit_spec in EXIT_SPECS],
        "leverages": [asdict(lev) for lev in LEVERAGE_SPECS],
        "baseConfigs": [asdict(base) for base in BASE_CONFIGS],
        "configs": [asdict(cfg) for cfg in full_configs],
        "defaultSelection": {
            "mode": "flat",
            "strategy": "s3",
            "exit": "e1",
            "lev": "3xS",
            "baseCfg": "500k_5pct",
        },
    }

    script = "window.DASH_META = " + json.dumps(meta, separators=(",", ":"), ensure_ascii=True) + ";\n"
    (OUT_DIR / "meta.js").write_text(script, encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    full_configs = build_full_configs()
    strategy3_context = load_strategy3_context()

    s1_meta = StrategyMeta(
        id="s1",
        title="Strategy 1 - 15-Min Opening Reversal (4V1)",
        subtitle="Original dashboard Strategy 1 trade universe preserved; exit, leverage, and risk variants are replayed on the same 1190 entries.",
        source_engine="temp/index.html initTrades(0) original legacy rows + replayed e1/e4/e8 sizing variants",
        max_trades=1190,
    )
    s2_meta = StrategyMeta(
        id="s2",
        title="Strategy 2 - Gap Fade Reversal",
        subtitle="Original dashboard Strategy 2 trade universe preserved; exit, leverage, and risk variants are replayed on the same 44792 entries.",
        source_engine="temp/index.html initTrades(1) original legacy rows + replayed e1/e4/e8 sizing variants",
        max_trades=44792,
    )
    s3_meta = StrategyMeta(
        id="s3",
        title=strategy3_context.title,
        subtitle=strategy3_context.subtitle,
        source_engine=strategy3_context.source_engine,
        max_trades=strategy3_context.max_trades_per_day,
    )

    strategies = [s1_meta, s2_meta, s3_meta]
    write_meta(strategies, full_configs)

    print("Building Strategy 1 from original dashboard rows...")
    generate_legacy_dashboard_strategy(s1_meta, full_configs, trade_index=0, entry_clock="10:00:00", source_variant_id="original_s1")

    print("Building Strategy 2 from original dashboard rows...")
    generate_legacy_dashboard_strategy(s2_meta, full_configs, trade_index=1, entry_clock="09:35:00", source_variant_id="original_s2")

    print("Building Strategy 3 from raw setups with multiprocessing backtests...")
    generate_strategy3_data(s3_meta, strategy3_context, full_configs)

    print("Build complete:")
    print(f"  Output folder: {OUT_DIR}")
    print("  Sources:")
    print("   - S1 from temp/index.html initTrades(0), preserving the original 1190 entries")
    print("   - S2 from temp/index.html initTrades(1), preserving the original 44792 entries")
    print("   - S3 from temp/search_strategy3_fixedrisk.py candidates + e1/e4/e8 re-simulation")


if __name__ == "__main__":
    main()
