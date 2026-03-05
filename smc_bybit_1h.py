#!/usr/bin/env python3
"""
SMC Trading Bot 1H — Bybit Futuros Perpetuos (Linear USDT)
3 Take Profits: R/R 3x | 5x | 8x  —  Distribución 25% | 35% | 40%
Apalancamiento recomendado calculado dinámicamente por operación

Requiere: pip install ccxt pandas numpy requests
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
#   CONFIGURACIÓN
# ╚══════════════════════════════════════════════════╝

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN_1H", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_TOPIC_ID  = 395

TZ_OFFSET = -6
TZ        = timezone(timedelta(hours=TZ_OFFSET))

TOP_N_SYMBOLS      = 100
SYMBOLS_REFRESH_H  = 6
MIN_VOLUME_USDT    = 1_000_000

TIMEFRAME          = "1h"
TREND_TF           = "4h"
CHECK_INTERVAL_SEC = 300

OB_LOOKBACK        = 60
FVG_MIN_SIZE       = 0.08
RR_MIN             = 3.0
CONFLUENCE_MIN     = 2
ALERT_COOLDOWN_H   = 8

# 3 Take Profits
TP1_RR  = 3.0  ;  TP1_PCT = 25
TP2_RR  = 5.0  ;  TP2_PCT = 35
TP3_RR  = 8.0  ;  TP3_PCT = 40

# Gestión de riesgo
RISK_PER_TRADE_PCT = 1.0   # % capital a arriesgar por operación
MAX_LEVERAGE       = 20    # techo máximo de apalancamiento
MAX_SL_PCT         = 5.0   # % máximo del SL respecto al precio

EMA_FAST = 50
EMA_SLOW = 200

BYBIT_API_KEY    = ""
BYBIT_API_SECRET = ""

# ╚══════════════════════════════════════════════════╝

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("SMC-1H")


@dataclass
class Signal:
    symbol: str;  direction: str
    entry: float; stop_loss: float
    tp1: float;   tp2: float;   tp3: float
    rr1: float;   rr2: float;   rr3: float
    sl_pct: float; leverage: int
    reason: str;  timeframe: str; timestamp: str
    funding_rate: float = 0.0
    open_interest_change: str = ""

@dataclass
class MarketContext:
    funding_rate: float = 0.0;  mark_price: float = 0.0
    open_interest: float = 0.0; oi_prev: float = 0.0


def build_exchange():
    p = {"enableRateLimit": True, "options": {"defaultType": "linear", "adjustForTimeDifference": True}}
    if BYBIT_API_KEY: p["apiKey"] = BYBIT_API_KEY; p["secret"] = BYBIT_API_SECRET
    return ccxt.bybit(p)


def fetch_top_symbols(exchange, top_n=TOP_N_SYMBOLS):
    log.info("Cargando pares desde Bybit...")
    try:
        markets = exchange.load_markets()
        perps   = [m for m in markets.values()
                   if m.get("linear") and m.get("active") and m.get("swap")
                   and m.get("quote") == "USDT" and m.get("settle") == "USDT"]
        if not perps: return _fallback()
        syms = [m["symbol"] for m in perps]
        try:    tickers = exchange.fetch_tickers(syms)
        except:
            tickers = {}
            for s in syms[:200]:
                try: tickers[s] = exchange.fetch_ticker(s); time.sleep(0.05)
                except: pass
        ranked = []
        for s, t in tickers.items():
            v = t.get("quoteVolume") or (t.get("baseVolume", 0) * t.get("last", 0))
            if v >= MIN_VOLUME_USDT: ranked.append((s, v))
        ranked.sort(key=lambda x: x[1], reverse=True)
        result = [s for s, _ in ranked[:top_n]]
        log.info(f"{len(result)} pares cargados. Top 3: {', '.join(result[:3])}")
        return result or _fallback()
    except Exception as e:
        log.error(f"Error pares: {e}"); return _fallback()


def _fallback():
    log.warning("Usando lista de respaldo.")
    return ["BTC/USDT:USDT","ETH/USDT:USDT","SOL/USDT:USDT","BNB/USDT:USDT",
            "XRP/USDT:USDT","DOGE/USDT:USDT","ADA/USDT:USDT","AVAX/USDT:USDT",
            "LINK/USDT:USDT","OP/USDT:USDT","ARB/USDT:USDT","MATIC/USDT:USDT",
            "DOT/USDT:USDT","LTC/USDT:USDT","ATOM/USDT:USDT","UNI/USDT:USDT",
            "SUI/USDT:USDT","TRX/USDT:USDT","FIL/USDT:USDT","INJ/USDT:USDT"]


def calc_leverage(entry: float, sl: float) -> int:
    """Apalancamiento óptimo = RISK% / SL%  — limitado a MAX_LEVERAGE."""
    sl_pct = abs(entry - sl) / entry * 100
    if sl_pct <= 0: return 1
    return max(1, min(MAX_LEVERAGE, int(RISK_PER_TRADE_PCT / sl_pct)))


def lev_label(lev: int) -> str:
    if lev >= 15: return "⚠️ Alto riesgo"
    if lev >= 10: return "🟡 Moderado-alto"
    if lev >= 5:  return "🟢 Moderado"
    return "✅ Conservador"


def send_telegram(msg: str) -> bool:
    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    body = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
    if TELEGRAM_TOPIC_ID: body["message_thread_id"] = TELEGRAM_TOPIC_ID
    try:
        r = requests.post(url, json=body, timeout=10)
        if r.status_code != 200: log.warning(f"TG {r.status_code}: {r.text[:150]}")
        return r.status_code == 200
    except Exception as e: log.error(f"TG error: {e}"); return False


def _now(): return datetime.now(TZ).strftime(f"%Y-%m-%d %H:%M (UTC{TZ_OFFSET:+d})")

def _pf(p):
    if p >= 1000: return f"{p:,.2f}"
    if p >= 1:    return f"{p:.4f}"
    return f"{p:.6f}"


def format_signal(s: Signal) -> str:
    e = "🟢" if s.direction == "LONG" else "🔴"
    a = "📈" if s.direction == "LONG" else "📉"
    d = s.symbol.replace(":USDT", " PERP")
    fl = ""
    if s.funding_rate:
        fr = s.funding_rate * 100
        fl = f"{'🔺' if fr>0 else '🔻'} <b>Funding:</b> <code>{fr:+.4f}%</code>\n"
    oi = f"📊 <b>OI:</b> {s.open_interest_change}\n" if s.open_interest_change else ""
    cf = "\n".join(f"  • {r}" for r in s.reason.split(" | "))
    return (
        f"{e} <b>SMC 1H — {s.direction}</b> {a}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 Bybit Futures | <b>{d}</b>\n"
        f"⏱  TF señales: <b>{s.timeframe}</b> | Tendencia: <b>{TREND_TF}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Entrada:</b>   <code>{_pf(s.entry)}</code>\n"
        f"🛑 <b>Stop Loss:</b> <code>{_pf(s.stop_loss)}</code>  <i>(-{s.sl_pct:.2f}%)</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>TP1 {TP1_PCT}% pos.:</b> <code>{_pf(s.tp1)}</code>  <b>R/R {s.rr1:.1f}x</b>\n"
        f"🎯 <b>TP2 {TP2_PCT}% pos.:</b> <code>{_pf(s.tp2)}</code>  <b>R/R {s.rr2:.1f}x</b>\n"
        f"🎯 <b>TP3 {TP3_PCT}% pos.:</b> <code>{_pf(s.tp3)}</code>  <b>R/R {s.rr3:.1f}x</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>Apalancamiento rec.:</b> <code>{s.leverage}x</code>  {lev_label(s.leverage)}\n"
        f"💡 <i>Con {s.leverage}x y {RISK_PER_TRADE_PCT}% capital arriesgado el TP1 duplica la posición</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{fl}{oi}"
        f"📋 <b>Confluencias:</b>\n{cf}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {s.timestamp}\n"
        f"⚠️ <i>Solo informativo. No es asesoría financiera.</i>"
    )


class SMCAnalyzer:
    def __init__(self, df):
        self.df = df.copy().reset_index(drop=True)
        df = self.df
        df["body"]    = abs(df["close"] - df["open"])
        df["rng"]     = df["high"] - df["low"]
        df["upper"]   = df["high"] - df[["close","open"]].max(axis=1)
        df["lower"]   = df[["close","open"]].min(axis=1) - df["low"]
        df["is_bull"] = df["close"] > df["open"]
        df["is_bear"] = df["close"] < df["open"]
        df["body_pct"]= df["body"] / df["rng"].replace(0, np.nan)

    def atr(self, p=14):
        df=self.df; hi,lo,cl=df["high"],df["low"],df["close"].shift(1)
        tr=pd.concat([hi-lo,(hi-cl).abs(),(lo-cl).abs()],axis=1).max(axis=1)
        return tr.rolling(p).mean().iloc[-1]

    def ema(self, p): return self.df["close"].ewm(span=p, adjust=False).mean()

    def trend_bias(self):
        c=self.df["close"].iloc[-1]; e50=self.ema(EMA_FAST).iloc[-1]; e200=self.ema(EMA_SLOW).iloc[-1]
        if c>e50 and e50>e200: return "bullish"
        if c<e50 and e50<e200: return "bearish"
        return "neutral"

    def swing_highs(self,n=5):
        h=self.df["high"]; return h==h.rolling(2*n+1,center=True).max()
    def swing_lows(self,n=5):
        l=self.df["low"]; return l==l.rolling(2*n+1,center=True).min()

    def market_structure(self):
        df=self.df
        hs=df.loc[self.swing_highs(),"high"].tail(3).values
        ls=df.loc[self.swing_lows(),"low"].tail(3).values
        if len(hs)<2 or len(ls)<2: return "ranging"
        if hs[-1]>hs[-2] and ls[-1]>ls[-2]: return "bullish"
        if hs[-1]<hs[-2] and ls[-1]<ls[-2]: return "bearish"
        return "ranging"

    def choch_bullish(self):
        hs=self.df.loc[self.swing_highs(),"high"].tail(4).values
        return len(hs)>=3 and hs[-2]>hs[-1] and self.df["close"].iloc[-1]>hs[-1]

    def choch_bearish(self):
        ls=self.df.loc[self.swing_lows(),"low"].tail(4).values
        return len(ls)>=3 and ls[-2]<ls[-1] and self.df["close"].iloc[-1]<ls[-1]

    def find_bullish_ob(self):
        df=self.df; ab=df["body"].rolling(20).mean()
        for i in range(len(df)-2,max(len(df)-OB_LOOKBACK,1),-1):
            if df.at[i,"is_bear"] and i+1<len(df):
                if df.at[i+1,"is_bull"] and df.at[i+1,"body"]>ab.iloc[i]*1.4:
                    return {"high":df.at[i,"high"],"low":df.at[i,"low"]}
        return None

    def find_bearish_ob(self):
        df=self.df; ab=df["body"].rolling(20).mean()
        for i in range(len(df)-2,max(len(df)-OB_LOOKBACK,1),-1):
            if df.at[i,"is_bull"] and i+1<len(df):
                if df.at[i+1,"is_bear"] and df.at[i+1,"body"]>ab.iloc[i]*1.4:
                    return {"high":df.at[i,"high"],"low":df.at[i,"low"]}
        return None

    def find_bullish_fvg(self):
        df=self.df
        for i in range(len(df)-3,max(len(df)-OB_LOOKBACK,0),-1):
            lo,hi=df.at[i+2,"low"],df.at[i,"high"]
            if lo>hi and (lo-hi)/hi*100>=FVG_MIN_SIZE: return {"high":lo,"low":hi}
        return None

    def find_bearish_fvg(self):
        df=self.df
        for i in range(len(df)-3,max(len(df)-OB_LOOKBACK,0),-1):
            hi,lo=df.at[i+2,"high"],df.at[i,"low"]
            if hi<lo and (lo-hi)/lo*100>=FVG_MIN_SIZE: return {"high":lo,"low":hi}
        return None

    def liq_sweep_down(self):
        df=self.df
        if len(df)<12: return False
        pl=df["low"].iloc[-12:-3].min()
        return df["low"].iloc[-2]<pl and df["close"].iloc[-1]>pl and df["lower"].iloc[-2]>df["body"].iloc[-2]*1.2

    def liq_sweep_up(self):
        df=self.df
        if len(df)<12: return False
        ph=df["high"].iloc[-12:-3].max()
        return df["high"].iloc[-2]>ph and df["close"].iloc[-1]<ph and df["upper"].iloc[-2]>df["body"].iloc[-2]*1.2

    def in_discount(self):
        df=self.df; eq=(df["high"].tail(OB_LOOKBACK).max()+df["low"].tail(OB_LOOKBACK).min())/2
        return df["close"].iloc[-1]<eq

    def in_premium(self):
        df=self.df; eq=(df["high"].tail(OB_LOOKBACK).max()+df["low"].tail(OB_LOOKBACK).min())/2
        return df["close"].iloc[-1]>eq

    def generate_signal(self, symbol, tf, ctx=None, trend_bias="neutral"):
        if trend_bias=="neutral": return None
        al=(trend_bias=="bullish"); ash=(trend_bias=="bearish")
        df=self.df; close=df["close"].iloc[-1]; atr=self.atr()
        st=self.market_structure()
        bob=self.find_bullish_ob(); bab=self.find_bearish_ob()
        bfv=self.find_bullish_fvg(); bafv=self.find_bearish_fvg()

        lr=[]
        if al:
            lr.append(f"📊 Tendencia Alcista (EMA{EMA_FAST}>EMA{EMA_SLOW})")
            if st=="bullish": lr.append("✅ Estructura Alcista (HH+HL)")
            if self.choch_bullish(): lr.append("🔀 CHoCH Alcista detectado")
            if bob:
                if bob["low"]*0.998<=close<=bob["high"]*1.003:
                    lr.append(f"🧱 OB Alcista [{_pf(bob['low'])} – {_pf(bob['high'])}]")
            if bfv and bfv["low"]*0.999<=close<=bfv["high"]*1.001:
                lr.append(f"⬜ FVG Alcista [{_pf(bfv['low'])} – {_pf(bfv['high'])}]")
            if self.liq_sweep_down(): lr.append("💧 Liquidity Sweep Bajista → Reversión")
            if self.in_discount():   lr.append("📉 Precio en Zona Descuento")
            if ctx and ctx.funding_rate<-0.0005:
                lr.append(f"💰 Funding negativo ({ctx.funding_rate*100:+.4f}%)")

        sr=[]
        if ash:
            sr.append(f"📊 Tendencia Bajista (EMA{EMA_FAST}<EMA{EMA_SLOW})")
            if st=="bearish": sr.append("✅ Estructura Bajista (LL+LH)")
            if self.choch_bearish(): sr.append("🔀 CHoCH Bajista detectado")
            if bab:
                if bab["low"]*0.998<=close<=bab["high"]*1.003:
                    sr.append(f"🧱 OB Bajista [{_pf(bab['low'])} – {_pf(bab['high'])}]")
            if bafv and bafv["low"]*0.999<=close<=bafv["high"]*1.001:
                sr.append(f"⬜ FVG Bajista [{_pf(bafv['low'])} – {_pf(bafv['high'])}]")
            if self.liq_sweep_up():  sr.append("💧 Liquidity Sweep Alcista → Reversión")
            if self.in_premium():    sr.append("📈 Precio en Zona Premium")
            if ctx and ctx.funding_rate>0.0005:
                sr.append(f"💰 Funding positivo ({ctx.funding_rate*100:+.4f}%)")

        oi_txt=""
        if ctx and ctx.oi_prev>0:
            oi_txt=f"{(ctx.open_interest-ctx.oi_prev)/ctx.oi_prev*100:+.2f}% vs escaneo anterior"

        def make(direction, reasons):
            entry=close
            if direction=="LONG":
                sl=(bob["low"]-atr*0.3) if bob else (df["low"].tail(15).min()-atr*0.2)
                sl=min(sl, entry-atr*0.5); risk=entry-sl
                if risk<=0: return None
                tp1=entry+risk*TP1_RR; tp2=entry+risk*TP2_RR; tp3=entry+risk*TP3_RR
            else:
                sl=(bab["high"]+atr*0.3) if bab else (df["high"].tail(15).max()+atr*0.2)
                sl=max(sl, entry+atr*0.5); risk=sl-entry
                if risk<=0: return None
                tp1=entry-risk*TP1_RR; tp2=entry-risk*TP2_RR; tp3=entry-risk*TP3_RR

            rr1=abs(tp1-entry)/risk; rr2=abs(tp2-entry)/risk; rr3=abs(tp3-entry)/risk
            if rr1<RR_MIN: return None
            sl_pct=abs(entry-sl)/entry*100
            if sl_pct>MAX_SL_PCT: return None
            lev=calc_leverage(entry,sl)
            return Signal(
                symbol=symbol, direction=direction,
                entry=round(entry,8), stop_loss=round(sl,8),
                tp1=round(tp1,8), tp2=round(tp2,8), tp3=round(tp3,8),
                rr1=round(rr1,1), rr2=round(rr2,1), rr3=round(rr3,1),
                sl_pct=round(sl_pct,2), leverage=lev,
                reason=" | ".join(reasons), timeframe=tf, timestamp=_now(),
                funding_rate=ctx.funding_rate if ctx else 0.0,
                open_interest_change=oi_txt,
            )

        if al and len(lr)>=CONFLUENCE_MIN and len(lr)>=len(sr): return make("LONG", lr)
        if ash and len(sr)>=CONFLUENCE_MIN: return make("SHORT", sr)
        return None


class BybitSMCBot:
    def __init__(self):
        self.exchange=build_exchange(); self.symbols=[]; self.last_signals={}
        self.oi_cache={}; self.last_refresh=0.0

    def refresh_if_needed(self, force=False):
        now=time.time()
        if not force and now-self.last_refresh<SYMBOLS_REFRESH_H*3600: return
        new=fetch_top_symbols(self.exchange); changed=set(new)!=set(self.symbols)
        self.symbols=new; self.last_refresh=now
        if changed or force:
            muestra="\n".join(f"  {i+1}. {s.replace(':USDT',' PERP')}" for i,s in enumerate(new[:20]))
            resto=f"\n  ... y {len(new)-20} más" if len(new)>20 else ""
            send_telegram(f"📋 <b>Pares 1H actualizados</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                          f"📊 <b>{len(new)} pares USDT PERP</b> — Top por volumen\n"
                          f"━━━━━━━━━━━━━━━━━━━━━━\n{muestra}{resto}")

    def fetch_ohlcv(self, symbol, tf, limit=220):
        try:
            raw=self.exchange.fetch_ohlcv(symbol,tf,limit=limit)
            if not raw: return None
            df=pd.DataFrame(raw,columns=["timestamp","open","high","low","close","volume"])
            df["timestamp"]=pd.to_datetime(df["timestamp"],unit="ms"); return df
        except Exception as e: log.warning(f"OHLCV {symbol}: {e}"); return None

    def fetch_ctx(self, symbol):
        ctx=MarketContext()
        try: ctx.mark_price=self.exchange.fetch_ticker(symbol).get("last",0.0) or 0.0
        except: pass
        try: ctx.funding_rate=self.exchange.fetch_funding_rate(symbol).get("fundingRate",0.0) or 0.0
        except: pass
        try:
            oi=self.exchange.fetch_open_interest(symbol).get("openInterestAmount",0.0) or 0.0
            ctx.open_interest=oi; ctx.oi_prev=self.oi_cache.get(symbol,oi)
            self.oi_cache[symbol]=oi
        except: pass
        return ctx

    def already_alerted(self, sig):
        key=f"{sig.symbol}_{sig.direction}"; now=time.time()
        if now-self.last_signals.get(key,0.0)<ALERT_COOLDOWN_H*3600: return True
        self.last_signals[key]=now; return False

    async def scan(self, symbol):
        df=self.fetch_ohlcv(symbol,TIMEFRAME)
        if df is None or len(df)<60: return
        dft=self.fetch_ohlcv(symbol,TREND_TF,limit=220)
        if dft is None or len(dft)<200: return
        bias=SMCAnalyzer(dft).trend_bias()
        if bias=="neutral": return
        ctx=self.fetch_ctx(symbol)
        sig=SMCAnalyzer(df).generate_signal(symbol,TIMEFRAME,ctx,trend_bias=bias)
        if sig and not self.already_alerted(sig):
            ok=send_telegram(format_signal(sig))
            log.info(f"{'OK' if ok else 'ERR'} | {symbol} {sig.direction} | Lev {sig.leverage}x | TP1 R/R {sig.rr1}x")

    async def run(self):
        log.info("="*55)
        log.info(f"  SMC Bot 1H | TF:{TIMEFRAME} Trend:{TREND_TF} | TPs:{TP1_RR}x/{TP2_RR}x/{TP3_RR}x")
        log.info("="*55)
        self.refresh_if_needed(force=True)
        send_telegram(
            "🤖 <b>SMC Bot 1H — Bybit Perpetuos</b> ✅\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱  Señales: <b>{TIMEFRAME}</b> | Tendencia: <b>{TREND_TF}</b>\n"
            f"📈 Filtro: <b>EMA{EMA_FAST}/EMA{EMA_SLOW}</b>\n"
            f"🎯 <b>3 TPs:</b> {TP1_RR}x ({TP1_PCT}%) | {TP2_RR}x ({TP2_PCT}%) | {TP3_RR}x ({TP3_PCT}%)\n"
            f"⚡ Apalancamiento máx: <b>{MAX_LEVERAGE}x</b> | Riesgo: <b>{RISK_PER_TRADE_PCT}%</b>\n"
            f"🔗 Confluencias: <b>{CONFLUENCE_MIN}</b> | Cooldown: <b>{ALERT_COOLDOWN_H}h</b>\n"
            f"🌐 Zona: <b>UTC{TZ_OFFSET:+d}</b> | Pares: <b>Top {TOP_N_SYMBOLS}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "<i>Solo señales a favor de tendencia · OB · FVG · CHoCH · Sweeps</i>"
        )
        while True:
            self.refresh_if_needed()
            t0=time.time()
            log.info(f"── {len(self.symbols)} pares [{datetime.now(TZ).strftime('%H:%M:%S')}] ──")
            for s in self.symbols: await self.scan(s); await asyncio.sleep(0.3)
            wait=max(0,CHECK_INTERVAL_SEC-(time.time()-t0))
            log.info(f"── próximo en {wait:.0f}s ──")
            await asyncio.sleep(wait)


if __name__=="__main__":
    bot=BybitSMCBot()
    try: asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Detenido."); send_telegram("🔴 <b>SMC Bot 1H detenido</b>")
