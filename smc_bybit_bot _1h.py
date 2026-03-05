#!/usr/bin/env python3
"""
SMC Trading Bot — Bybit Futuros Perpetuos (Linear USDT)
Estrategia: Smart Money Concepts
Envía señales LONG / SHORT a Telegram con entrada, SL, TP y R/R

Pares: Top 100 USDT perpetuos de Bybit ordenados por volumen (auto-actualizable)

Requiere:
    pip install ccxt pandas numpy requests
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
import pandas as pd
import numpy as np
import ccxt
import requests
from dataclasses import dataclass

# ╔══════════════════════════════════════════════════╗
#   CONFIGURACIÓN  — edita solo esta sección
# ╚══════════════════════════════════════════════════╝

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
# Topic de Telegram — None = chat directo, número = topic específico
TELEGRAM_TOPIC_ID  = 395   # ej: TELEGRAM_TOPIC_ID = 123

# Zona horaria
TZ_OFFSET = -6              # UTC-6 = CST México
TZ        = timezone(timedelta(hours=TZ_OFFSET))

# ── Pares dinámicos ──────────────────────────────────
TOP_N_SYMBOLS      = 100     # cuántos pares del top por volumen monitorear
SYMBOLS_REFRESH_H  = 6       # cada cuántas horas refrescar la lista de pares
MIN_VOLUME_USDT    = 1_000_000  # volumen mínimo en USDT para incluir un par

# ── Timeframes ───────────────────────────────────────
TIMEFRAME          = "1H"   # TF de señales
TREND_TF           = "4h"    # TF de tendencia (debe ser mayor que TIMEFRAME)
CHECK_INTERVAL_SEC = 60      # segundos entre escaneos

# ── Parámetros SMC ───────────────────────────────────
OB_LOOKBACK        = 60
FVG_MIN_SIZE       = 0.08    # % mínimo tamaño FVG
RR_MIN             = 2.0     # R/R mínimo
CONFLUENCE_MIN     = 2       # confluencias mínimas
ALERT_COOLDOWN_H   = 4       # horas de cooldown por par/dirección

# ── Filtro de Tendencia EMA ──────────────────────────
EMA_FAST = 50
EMA_SLOW = 200

# ── Bybit API keys (opcional) ────────────────────────
BYBIT_API_KEY    = ""
BYBIT_API_SECRET = ""

# ╚══════════════════════════════════════════════════╝

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("SMC-Bybit")


# ─────────────────────────────────────────────────────
#  DATA CLASSES
# ─────────────────────────────────────────────────────
@dataclass
class Signal:
    symbol:               str
    direction:            str
    entry:                float
    stop_loss:            float
    take_profit:          float
    reason:               str
    timeframe:            str
    rr:                   float
    timestamp:            str
    funding_rate:         float = 0.0
    open_interest_change: str   = ""


@dataclass
class MarketContext:
    funding_rate:  float = 0.0
    mark_price:    float = 0.0
    open_interest: float = 0.0
    oi_prev:       float = 0.0


# ─────────────────────────────────────────────────────
#  BYBIT EXCHANGE
# ─────────────────────────────────────────────────────
def build_exchange() -> ccxt.bybit:
    params = {
        "enableRateLimit": True,
        "options": {
            "defaultType": "linear",
            "adjustForTimeDifference": True,
        },
    }
    if BYBIT_API_KEY and BYBIT_API_SECRET:
        params["apiKey"] = BYBIT_API_KEY
        params["secret"] = BYBIT_API_SECRET
    return ccxt.bybit(params)


# ─────────────────────────────────────────────────────
#  CARGA DINÁMICA DE PARES — Top N por volumen
# ─────────────────────────────────────────────────────
def fetch_top_symbols(exchange: ccxt.bybit, top_n: int = TOP_N_SYMBOLS) -> list:
    """
    Consulta todos los mercados lineales USDT de Bybit,
    los ordena por volumen en USDT de las últimas 24h
    y devuelve los top_n con mayor liquidez.
    """
    log.info("Cargando lista de pares desde Bybit...")
    try:
        # Cargar todos los mercados del exchange
        markets = exchange.load_markets()

        # Filtrar solo perpetuos lineales USDT activos
        usdt_perps = [
            m for m in markets.values()
            if m.get("linear")          # perpetuo lineal
            and m.get("active")         # activo
            and m.get("swap")           # es un swap/perpetuo
            and m.get("quote") == "USDT"
            and m.get("settle") == "USDT"
        ]

        if not usdt_perps:
            log.warning("No se encontraron pares USDT perpetuos, usando lista de respaldo")
            return _fallback_symbols()

        # Obtener tickers para saber el volumen 24h de cada par
        symbols_list = [m["symbol"] for m in usdt_perps]
        log.info(f"Obteniendo volúmenes de {len(symbols_list)} pares...")

        # fetch_tickers puede traer todos de una vez (más eficiente)
        try:
            tickers = exchange.fetch_tickers(symbols_list)
        except Exception:
            # Si falla en bloque, intentar de uno en uno (más lento pero más robusto)
            tickers = {}
            for sym in symbols_list[:200]:  # limitar para no sobrecargar
                try:
                    t = exchange.fetch_ticker(sym)
                    tickers[sym] = t
                    time.sleep(0.05)
                except Exception:
                    pass

        # Calcular volumen en USDT = quoteVolume o baseVolume * last
        ranked = []
        for sym, ticker in tickers.items():
            vol = ticker.get("quoteVolume") or 0
            if not vol:
                vol = (ticker.get("baseVolume") or 0) * (ticker.get("last") or 0)
            if vol >= MIN_VOLUME_USDT:
                ranked.append((sym, vol))

        # Ordenar de mayor a menor volumen
        ranked.sort(key=lambda x: x[1], reverse=True)
        top_symbols = [s for s, _ in ranked[:top_n]]

        log.info(f"Top {len(top_symbols)} pares cargados por volumen.")
        if top_symbols:
            top3 = ", ".join(top_symbols[:3])
            log.info(f"Top 3: {top3} ...")

        return top_symbols if top_symbols else _fallback_symbols()

    except Exception as e:
        log.error(f"Error cargando pares dinámicos: {e}")
        return _fallback_symbols()


def _fallback_symbols() -> list:
    """Lista de respaldo si falla la carga dinámica."""
    log.warning("Usando lista de respaldo de 20 pares principales.")
    return [
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "BNB/USDT:USDT", "XRP/USDT:USDT", "DOGE/USDT:USDT",
        "ADA/USDT:USDT", "AVAX/USDT:USDT", "LINK/USDT:USDT",
        "OP/USDT:USDT",  "ARB/USDT:USDT", "MATIC/USDT:USDT",
        "DOT/USDT:USDT", "LTC/USDT:USDT", "ATOM/USDT:USDT",
        "UNI/USDT:USDT", "SUI/USDT:USDT", "TRX/USDT:USDT",
        "FIL/USDT:USDT", "INJ/USDT:USDT", "XAUT/USDT:USDT",
    ]


# ─────────────────────────────────────────────────────
#  TELEGRAM
# ─────────────────────────────────────────────────────
def send_telegram(message: str) -> bool:
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
    }
    if TELEGRAM_TOPIC_ID is not None:
        payload["message_thread_id"] = TELEGRAM_TOPIC_ID
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log.warning(f"Telegram HTTP {r.status_code}: {r.text[:200]}")
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


def _now_local() -> str:
    return datetime.now(TZ).strftime(f"%Y-%m-%d %H:%M (UTC{TZ_OFFSET:+d})")


def _price_fmt(price: float) -> str:
    if price >= 1000:
        return f"{price:,.2f}"
    elif price >= 1:
        return f"{price:.4f}"
    else:
        return f"{price:.6f}"


def format_signal(sig: Signal) -> str:
    emoji   = "🟢" if sig.direction == "LONG" else "🔴"
    arrow   = "📈" if sig.direction == "LONG" else "📉"
    display = sig.symbol.replace(":USDT", " PERP").replace(":USDC", " PERP")

    funding_line = ""
    if sig.funding_rate != 0.0:
        fr_pct   = sig.funding_rate * 100
        fr_emoji = "🔺" if fr_pct > 0 else "🔻"
        funding_line = f"{fr_emoji} <b>Funding:</b>  <code>{fr_pct:+.4f}%</code>\n"

    oi_line      = f"📊 <b>OI:</b>  {sig.open_interest_change}\n" if sig.open_interest_change else ""
    confluencias = "\n".join(f"  • {r}" for r in sig.reason.split(" | "))

    return (
        f"{emoji} <b>SMC PERPETUO {sig.direction}</b> {arrow}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 <b>Exchange:</b> Bybit Futures\n"
        f"🪙 <b>Par:</b>  <code>{display}</code>\n"
        f"⏱  <b>TF:</b>   {sig.timeframe}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Entrada:</b>    <code>{_price_fmt(sig.entry)}</code>\n"
        f"🛑 <b>Stop Loss:</b>  <code>{_price_fmt(sig.stop_loss)}</code>\n"
        f"🎯 <b>Take Profit:</b> <code>{_price_fmt(sig.take_profit)}</code>\n"
        f"⚖️  <b>R/R:</b>      {sig.rr:.1f}x\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{funding_line}"
        f"{oi_line}"
        f"📋 <b>Confluencias:</b>\n{confluencias}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {sig.timestamp}\n"
        f"⚠️ <i>Solo informativo. No es asesoría financiera.</i>"
    )


def format_symbols_update(symbols: list, refresh: bool = False) -> str:
    """Mensaje de Telegram al cargar/actualizar la lista de pares."""
    titulo  = "🔄 <b>Lista de pares actualizada</b>" if refresh else "📋 <b>Pares monitoreados</b>"
    muestra = "\n".join(
        f"  {i+1}. {s.replace(':USDT', ' PERP')}"
        for i, s in enumerate(symbols[:20])
    )
    resto   = f"\n  ... y {len(symbols) - 20} más" if len(symbols) > 20 else ""
    return (
        f"{titulo}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Total: <b>{len(symbols)} pares USDT PERP</b>\n"
        f"🏆 Ordenados por volumen 24h\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{muestra}{resto}"
    )


# ─────────────────────────────────────────────────────
#  ANÁLISIS SMC
# ─────────────────────────────────────────────────────
class SMCAnalyzer:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy().reset_index(drop=True)
        self._calc_basics()

    def _calc_basics(self):
        df = self.df
        df["body"]     = abs(df["close"] - df["open"])
        df["rng"]      = df["high"] - df["low"]
        df["upper"]    = df["high"] - df[["close", "open"]].max(axis=1)
        df["lower"]    = df[["close", "open"]].min(axis=1) - df["low"]
        df["is_bull"]  = df["close"] > df["open"]
        df["is_bear"]  = df["close"] < df["open"]
        df["body_pct"] = df["body"] / df["rng"].replace(0, np.nan)

    def atr(self, period: int = 14) -> float:
        df = self.df
        hi, lo, cl = df["high"], df["low"], df["close"].shift(1)
        tr = pd.concat([hi - lo, (hi - cl).abs(), (lo - cl).abs()], axis=1).max(axis=1)
        return tr.rolling(period).mean().iloc[-1]

    def ema(self, period: int) -> pd.Series:
        return self.df["close"].ewm(span=period, adjust=False).mean()

    def trend_bias(self) -> str:
        close  = self.df["close"].iloc[-1]
        ema50  = self.ema(EMA_FAST).iloc[-1]
        ema200 = self.ema(EMA_SLOW).iloc[-1]
        if close > ema50 and ema50 > ema200:
            return "bullish"
        elif close < ema50 and ema50 < ema200:
            return "bearish"
        return "neutral"

    def trend_strength(self) -> str:
        ema50  = self.ema(EMA_FAST).iloc[-1]
        ema200 = self.ema(EMA_SLOW).iloc[-1]
        pct    = abs(ema50 - ema200) / ema200 * 100
        if pct > 3:   return "fuerte"
        if pct > 1:   return "moderada"
        return "débil"

    def swing_highs(self, n: int = 5) -> pd.Series:
        h = self.df["high"]
        return h == h.rolling(2 * n + 1, center=True).max()

    def swing_lows(self, n: int = 5) -> pd.Series:
        l = self.df["low"]
        return l == l.rolling(2 * n + 1, center=True).min()

    def market_structure(self) -> str:
        df         = self.df
        last_highs = df.loc[self.swing_highs(), "high"].tail(3).values
        last_lows  = df.loc[self.swing_lows(),  "low"].tail(3).values
        if len(last_highs) < 2 or len(last_lows) < 2:
            return "ranging"
        hh = last_highs[-1] > last_highs[-2]
        hl = last_lows[-1]  > last_lows[-2]
        ll = last_lows[-1]  < last_lows[-2]
        lh = last_highs[-1] < last_highs[-2]
        if hh and hl: return "bullish"
        if ll and lh: return "bearish"
        return "ranging"

    def choch_bullish(self) -> bool:
        df    = self.df
        highs = df.loc[self.swing_highs(), "high"].tail(4).values
        if len(highs) < 3: return False
        return (highs[-2] > highs[-1]) and (df["close"].iloc[-1] > highs[-1])

    def choch_bearish(self) -> bool:
        df   = self.df
        lows = df.loc[self.swing_lows(), "low"].tail(4).values
        if len(lows) < 3: return False
        return (lows[-2] < lows[-1]) and (df["close"].iloc[-1] < lows[-1])

    def find_bullish_ob(self) -> Optional[dict]:
        df       = self.df
        avg_body = df["body"].rolling(20).mean()
        for i in range(len(df) - 2, max(len(df) - OB_LOOKBACK, 1), -1):
            if df.at[i, "is_bear"] and i + 1 < len(df):
                if df.at[i+1, "is_bull"] and df.at[i+1, "body"] > avg_body.iloc[i] * 1.4:
                    return {"high": df.at[i, "high"], "low": df.at[i, "low"]}
        return None

    def find_bearish_ob(self) -> Optional[dict]:
        df       = self.df
        avg_body = df["body"].rolling(20).mean()
        for i in range(len(df) - 2, max(len(df) - OB_LOOKBACK, 1), -1):
            if df.at[i, "is_bull"] and i + 1 < len(df):
                if df.at[i+1, "is_bear"] and df.at[i+1, "body"] > avg_body.iloc[i] * 1.4:
                    return {"high": df.at[i, "high"], "low": df.at[i, "low"]}
        return None

    def find_bullish_fvg(self) -> Optional[dict]:
        df = self.df
        for i in range(len(df) - 3, max(len(df) - OB_LOOKBACK, 0), -1):
            gap_lo, gap_hi = df.at[i+2, "low"], df.at[i, "high"]
            if gap_lo > gap_hi and (gap_lo - gap_hi) / gap_hi * 100 >= FVG_MIN_SIZE:
                return {"high": gap_lo, "low": gap_hi}
        return None

    def find_bearish_fvg(self) -> Optional[dict]:
        df = self.df
        for i in range(len(df) - 3, max(len(df) - OB_LOOKBACK, 0), -1):
            gap_hi, gap_lo = df.at[i+2, "high"], df.at[i, "low"]
            if gap_hi < gap_lo and (gap_lo - gap_hi) / gap_lo * 100 >= FVG_MIN_SIZE:
                return {"high": gap_lo, "low": gap_hi}
        return None

    def liquidity_sweep_down(self) -> bool:
        df = self.df
        if len(df) < 12: return False
        prev_low = df["low"].iloc[-12:-3].min()
        return (df["low"].iloc[-2] < prev_low and
                df["close"].iloc[-1] > prev_low and
                df["lower"].iloc[-2] > df["body"].iloc[-2] * 1.2)

    def liquidity_sweep_up(self) -> bool:
        df = self.df
        if len(df) < 12: return False
        prev_high = df["high"].iloc[-12:-3].max()
        return (df["high"].iloc[-2] > prev_high and
                df["close"].iloc[-1] < prev_high and
                df["upper"].iloc[-2] > df["body"].iloc[-2] * 1.2)

    def in_discount(self) -> bool:
        df = self.df
        eq = (df["high"].tail(OB_LOOKBACK).max() + df["low"].tail(OB_LOOKBACK).min()) / 2
        return df["close"].iloc[-1] < eq

    def in_premium(self) -> bool:
        df = self.df
        eq = (df["high"].tail(OB_LOOKBACK).max() + df["low"].tail(OB_LOOKBACK).min()) / 2
        return df["close"].iloc[-1] > eq

    def generate_signal(self, symbol: str, tf: str,
                        ctx: Optional[MarketContext] = None,
                        trend_bias: str = "neutral") -> Optional[Signal]:

        if trend_bias == "neutral":
            return None

        allow_long  = (trend_bias == "bullish")
        allow_short = (trend_bias == "bearish")

        df        = self.df
        close     = df["close"].iloc[-1]
        atr_val   = self.atr()
        structure = self.market_structure()
        bull_ob   = self.find_bullish_ob()
        bear_ob   = self.find_bearish_ob()
        bull_fvg  = self.find_bullish_fvg()
        bear_fvg  = self.find_bearish_fvg()

        long_r = []
        if allow_long:
            long_r.append(f"📊 Tendencia Alcista (EMA{EMA_FAST}>EMA{EMA_SLOW})")
            if structure == "bullish":
                long_r.append("✅ Estructura Alcista (HH+HL)")
            if self.choch_bullish():
                long_r.append("🔀 CHoCH Alcista detectado")
            if bull_ob:
                z_hi, z_lo = bull_ob["high"], bull_ob["low"]
                if z_lo * 0.998 <= close <= z_hi * 1.003:
                    long_r.append(f"🧱 OB Alcista [{_price_fmt(z_lo)} – {_price_fmt(z_hi)}]")
            if bull_fvg and bull_fvg["low"] * 0.999 <= close <= bull_fvg["high"] * 1.001:
                long_r.append(f"⬜ FVG Alcista [{_price_fmt(bull_fvg['low'])} – {_price_fmt(bull_fvg['high'])}]")
            if self.liquidity_sweep_down():
                long_r.append("💧 Liquidity Sweep Bajista → Reversión")
            if self.in_discount():
                long_r.append("📉 Precio en Zona Descuento (<50% rango)")
            if ctx and ctx.funding_rate < -0.0005:
                long_r.append(f"💰 Funding negativo ({ctx.funding_rate*100:+.4f}%) — sesgo alcista")

        short_r = []
        if allow_short:
            short_r.append(f"📊 Tendencia Bajista (EMA{EMA_FAST}<EMA{EMA_SLOW})")
            if structure == "bearish":
                short_r.append("✅ Estructura Bajista (LL+LH)")
            if self.choch_bearish():
                short_r.append("🔀 CHoCH Bajista detectado")
            if bear_ob:
                z_hi, z_lo = bear_ob["high"], bear_ob["low"]
                if z_lo * 0.998 <= close <= z_hi * 1.003:
                    short_r.append(f"🧱 OB Bajista [{_price_fmt(z_lo)} – {_price_fmt(z_hi)}]")
            if bear_fvg and bear_fvg["low"] * 0.999 <= close <= bear_fvg["high"] * 1.001:
                short_r.append(f"⬜ FVG Bajista [{_price_fmt(bear_fvg['low'])} – {_price_fmt(bear_fvg['high'])}]")
            if self.liquidity_sweep_up():
                short_r.append("💧 Liquidity Sweep Alcista → Reversión")
            if self.in_premium():
                short_r.append("📈 Precio en Zona Premium (>50% rango)")
            if ctx and ctx.funding_rate > 0.0005:
                short_r.append(f"💰 Funding positivo ({ctx.funding_rate*100:+.4f}%) — sesgo bajista")

        oi_txt = ""
        if ctx and ctx.oi_prev > 0:
            oi_chg = (ctx.open_interest - ctx.oi_prev) / ctx.oi_prev * 100
            oi_txt = f"{oi_chg:+.2f}% vs escaneo anterior"

        def make_signal(direction: str, reasons: list) -> Optional[Signal]:
            entry = close
            if direction == "LONG":
                sl   = (bull_ob["low"] - atr_val * 0.3) if bull_ob else (df["low"].tail(15).min() - atr_val * 0.2)
                sl   = min(sl, entry - atr_val * 0.5)
                risk = entry - sl
                if risk <= 0: return None
                tp = entry + risk * RR_MIN
            else:
                sl   = (bear_ob["high"] + atr_val * 0.3) if bear_ob else (df["high"].tail(15).max() + atr_val * 0.2)
                sl   = max(sl, entry + atr_val * 0.5)
                risk = sl - entry
                if risk <= 0: return None
                tp = entry - risk * RR_MIN

            rr_calc = abs(tp - entry) / risk
            if rr_calc < RR_MIN: return None

            return Signal(
                symbol=symbol, direction=direction,
                entry=round(entry, 8), stop_loss=round(sl, 8),
                take_profit=round(tp, 8),
                reason=" | ".join(reasons),
                timeframe=tf, rr=round(rr_calc, 2),
                timestamp=_now_local(),
                funding_rate=ctx.funding_rate if ctx else 0.0,
                open_interest_change=oi_txt,
            )

        if allow_long and len(long_r) >= CONFLUENCE_MIN and len(long_r) >= len(short_r):
            return make_signal("LONG", long_r)
        elif allow_short and len(short_r) >= CONFLUENCE_MIN:
            return make_signal("SHORT", short_r)
        return None


# ─────────────────────────────────────────────────────
#  BOT PRINCIPAL
# ─────────────────────────────────────────────────────
class BybitSMCBot:
    def __init__(self):
        self.exchange          = build_exchange()
        self.symbols:    list  = []
        self.last_signals: dict = {}
        self.oi_cache:     dict = {}
        self.last_symbols_refresh: float = 0.0

    # ── Actualizar lista de pares ────────
    def refresh_symbols_if_needed(self, force: bool = False) -> bool:
        """Recarga el top N de pares si han pasado SYMBOLS_REFRESH_H horas."""
        now     = time.time()
        elapsed = now - self.last_symbols_refresh
        if not force and elapsed < SYMBOLS_REFRESH_H * 3600:
            return False

        new_symbols = fetch_top_symbols(self.exchange, TOP_N_SYMBOLS)
        changed     = set(new_symbols) != set(self.symbols)
        self.symbols = new_symbols
        self.last_symbols_refresh = now

        if changed or force:
            log.info(f"Lista de pares actualizada: {len(self.symbols)} pares")
            send_telegram(format_symbols_update(self.symbols, refresh=not force))

        return True

    # ── OHLCV ────────────────────────────
    def fetch_ohlcv(self, symbol: str, tf: str, limit: int = 220) -> Optional[pd.DataFrame]:
        try:
            raw = self.exchange.fetch_ohlcv(symbol, tf, limit=limit)
            if not raw:
                return None
            df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            return df
        except Exception as e:
            log.warning(f"OHLCV error {symbol}: {e}")
            return None

    # ── Market Context ───────────────────
    def fetch_market_context(self, symbol: str) -> MarketContext:
        ctx = MarketContext()
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            ctx.mark_price = ticker.get("last", 0.0) or 0.0
        except Exception:
            pass
        try:
            fr_data = self.exchange.fetch_funding_rate(symbol)
            ctx.funding_rate = fr_data.get("fundingRate", 0.0) or 0.0
        except Exception:
            pass
        try:
            oi_data = self.exchange.fetch_open_interest(symbol)
            ctx.open_interest = oi_data.get("openInterestAmount", 0.0) or 0.0
            ctx.oi_prev = self.oi_cache.get(symbol, ctx.open_interest)
            self.oi_cache[symbol] = ctx.open_interest
        except Exception:
            pass
        return ctx

    # ── Anti-spam ────────────────────────
    def already_alerted(self, signal: Signal) -> bool:
        key  = f"{signal.symbol}_{signal.direction}"
        now  = time.time()
        last = self.last_signals.get(key, 0.0)
        if now - last < ALERT_COOLDOWN_H * 3600:
            return True
        self.last_signals[key] = now
        return False

    # ── Escaneo de un símbolo ────────────
    async def scan_symbol(self, symbol: str):
        df = self.fetch_ohlcv(symbol, TIMEFRAME)
        if df is None or len(df) < 60:
            return

        df_trend = self.fetch_ohlcv(symbol, TREND_TF, limit=220)
        if df_trend is None or len(df_trend) < 200:
            return

        bias = SMCAnalyzer(df_trend).trend_bias()
        if bias == "neutral":
            return

        ctx    = self.fetch_market_context(symbol)
        signal = SMCAnalyzer(df).generate_signal(symbol, TIMEFRAME, ctx, trend_bias=bias)

        if signal and not self.already_alerted(signal):
            ok     = send_telegram(format_signal(signal))
            status = "ENVIADA" if ok else "FALLO"
            log.info(f"[{status}] {symbol} {signal.direction} | {bias} | R/R {signal.rr}x")
        else:
            motivo = "cooldown" if signal else f"sin confluencias ({bias})"
            log.debug(f"Sin señal: {symbol} | {motivo}")

    # ── Ciclo principal ──────────────────
    async def run(self):
        log.info("=" * 60)
        log.info("  SMC Bot — Bybit Perpetuos USDT | Top 100 por volumen")
        log.info(f"  TF señales: {TIMEFRAME} | TF tendencia: {TREND_TF} | UTC{TZ_OFFSET:+d}")
        log.info(f"  R/R min: {RR_MIN}x | Confluencias: {CONFLUENCE_MIN} | Cooldown: {ALERT_COOLDOWN_H}h")
        log.info("=" * 60)

        # Carga inicial de pares
        self.refresh_symbols_if_needed(force=True)

        topic_txt = f"Topic {TELEGRAM_TOPIC_ID}" if TELEGRAM_TOPIC_ID else "Chat directo"

        send_telegram(
            "🤖 <b>SMC Bot — Bybit Perpetuos</b> ✅\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱  Señales: <b>{TIMEFRAME}</b> | Tendencia: <b>{TREND_TF}</b>\n"
            f"📈 Filtro: <b>EMA{EMA_FAST} / EMA{EMA_SLOW}</b>\n"
            f"⚖️  R/R mínimo: <b>{RR_MIN}x</b>\n"
            f"🔗  Confluencias: <b>{CONFLUENCE_MIN}</b>\n"
            f"⏲️  Cooldown: <b>{ALERT_COOLDOWN_H}h</b>\n"
            f"🌐 Zona horaria: <b>UTC{TZ_OFFSET:+d}</b>\n"
            f"💬 Destino: <b>{topic_txt}</b>\n"
            f"🏆 Pares: <b>Top {TOP_N_SYMBOLS} USDT PERP por volumen</b>\n"
            f"🔄 Actualización lista: cada <b>{SYMBOLS_REFRESH_H}h</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "<i>Solo señales a favor de tendencia</i>\n"
            "<i>OB · FVG · CHoCH · Liquidity Sweeps · Funding · OI</i>"
        )

        while True:
            # Refrescar pares si toca
            self.refresh_symbols_if_needed()

            t0         = time.time()
            hora_local = datetime.now(TZ).strftime("%H:%M:%S")
            log.info(f"── Escaneando {len(self.symbols)} pares [{hora_local} UTC{TZ_OFFSET:+d}] ──")

            for symbol in self.symbols:
                await self.scan_symbol(symbol)
                await asyncio.sleep(0.3)

            elapsed = time.time() - t0
            wait    = max(0, CHECK_INTERVAL_SEC - elapsed)
            log.info(f"── Ciclo en {elapsed:.1f}s — próximo en {wait:.0f}s ──")
            await asyncio.sleep(wait)


# ─────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = BybitSMCBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Bot detenido por el usuario.")
        send_telegram("🔴 <b>SMC Bot detenido</b> — Señales pausadas.")
