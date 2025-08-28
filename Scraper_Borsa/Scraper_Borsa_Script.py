#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web Scraper ottimizzato per titoli Borsa Italiana
- Usa Yahoo Finance (via yfinance) per storici
- Evita scraping aggressivo di siti con ToS restrittivi
- Scarica i titoli a blocchi per evitare rate limit
- Mostra una progress bar per i batch
Requirements:
    pip install pandas yfinance requests tenacity tqdm
"""
import argparse, os, time, sys, random
import pandas as pd
import yfinance as yf
from tenacity import retry, wait_exponential, stop_after_attempt
from tqdm import tqdm


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", default="FTSEMIB.MI", help="Ticker indice (FTSEMIB.MI, FTSESTAR.MI, ecc.)")
    ap.add_argument("--period", default="1y", help="Periodo storico (1mo,3mo,6mo,1y,5y,max)")
    ap.add_argument("--interval", default="1d", help="Intervallo (1d,1wk,1mo)")
    ap.add_argument("--batch-size", type=int, default=5, help="Numero di ticker per batch (default 5)")
    return ap.parse_args()


def get_index_constituents(index_ticker="FTSEMIB.MI"):
    # yfinance non sempre fornisce componenti, fallback manuale
    try:
        idx = yf.Ticker(index_ticker)
        comps = idx.constituents
        if isinstance(comps, pd.DataFrame) and "Symbol" in comps.columns:
            return comps["Symbol"].dropna().unique().tolist()
    except Exception:
        pass
    return ["ENI.MI", "ENEL.MI", "ISP.MI", "UCG.MI", "LDO.MI", "STLAM.MI"]


@retry(wait=wait_exponential(multiplier=2, min=5, max=120), stop=stop_after_attempt(5))
def download_batch(tickers, period="1y", interval="1d"):
    try:
        df = yf.download(tickers, period=period, interval=interval, group_by="ticker", auto_adjust=False, progress=False)
        results = []

        if isinstance(df.columns, pd.MultiIndex):
            for t in tickers:
                try:
                    dft = df[t].dropna().reset_index()
                    dft["Ticker"] = t
                    results.append(dft)
                except Exception as e:
                    with open("errors.log", "a") as ferr:
                        ferr.write(f"Errore {t}: {e}\n")
        else:
            df = df.dropna().reset_index()
            df["Ticker"] = tickers[0]
            results.append(df)

        return results
    except Exception as e:
        # Se errore contiene indizio di rate limit → forzare retry
        if "Too Many Requests" in str(e) or "rate limit" in str(e).lower():
            raise
        else:
            raise


def main():
    args = parse_args()

    # Determina la cartella dello script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = script_dir  # Salva direttamente nella cartella dello script

    tickers = get_index_constituents(args.index)
    print(f"Scarico {len(tickers)} titoli in batch da {args.batch_size}...")

    rows = []
    batches = [tickers[i:i + args.batch_size] for i in range(0, len(tickers), args.batch_size)]

    for batch in tqdm(batches, desc="Download batch", unit="batch"):
        try:
            batch_data = download_batch(batch, args.period, args.interval)
            rows.extend(batch_data)
        except Exception as e:
            with open("errors.log", "a") as ferr:
                ferr.write(f"Errore batch {batch}: {e}\n")
        time.sleep(random.uniform(1, 3))

    # salvataggio file singoli
    for df in rows:
        if df.empty:
            continue
        ticker = df["Ticker"].iloc[0]
        safe_ticker = ticker.replace(".", "_")
        outp = os.path.join(output_dir, f"{safe_ticker}_{args.period}_{args.interval}.csv")
        df.to_csv(outp, index=False)

    # file aggregato e statistiche
    if rows:
        all_df = pd.concat(rows, ignore_index=True)
        all_df.to_csv(os.path.join(output_dir, "all_history.csv"), index=False)

        piv = (all_df.groupby("Ticker")["Close"]
               .agg(["min", "max", "mean", "std"])
               .reset_index())
        piv.to_csv(os.path.join(output_dir, "summary_stats.csv"), index=False)

    print("Fatto. Risultati in:", output_dir)


if __name__ == "__main__":
    main()
