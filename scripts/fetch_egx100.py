"""
EGX 100 Stock Data Collection - Standalone Script
==================================================
Fetches data for all EGX 100 tickers using yfinance.
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# EGX 100 Tickers
EGX100_TICKERS = [
    # Banking
    'COMI.CA', 'CIEB.CA', 'HDBK.CA', 'FAIT.CA', 'ADIB.CA', 'SAUD.CA', 'EGBE.CA', 'EXPA.CA',
    # Financial Services
    'HRHO.CA', 'EFIH.CA', 'FWRY.CA', 'BTFH.CA', 'CICH.CA',
    # Real Estate
    'TMGH.CA', 'PHDC.CA', 'OCDI.CA', 'MASR.CA', 'HELI.CA', 'EMFD.CA', 'AMER.CA', 'EHDR.CA', 'ELKA.CA', 'ELSH.CA', 'ZMID.CA', 'EGTS.CA', 'MENA.CA',
    # Construction
    'ORAS.CA', 'ARCC.CA', 'SCEM.CA', 'MCQE.CA', 'SVCE.CA', 'ENGC.CA', 'GGCC.CA',
    # Industrial
    'SWDY.CA', 'EGAL.CA', 'ELEC.CA', 'ORWE.CA', 'LCSW.CA', 'ECAP.CA', 'PRCL.CA', 'CERA.CA', 'KABO.CA', 'DSCW.CA', 'MTIE.CA',
    # Chemicals
    'ABUK.CA', 'MFPC.CA', 'SKPC.CA', 'AMOC.CA', 'EGCH.CA', 'ICFC.CA', 'COSG.CA',
    # Oil & Gas
    'ASCM.CA', 'MOIL.CA', 'TAQA.CA',
    # Telecom
    'ETEL.CA', 'RAYA.CA',
    # Consumer
    'EAST.CA', 'JUFO.CA', 'EFID.CA', 'POUL.CA', 'ISMA.CA', 'MPCO.CA', 'SUGR.CA', 'IFAP.CA',
    # Healthcare
    'ISPH.CA', 'CLHO.CA', 'PHAR.CA', 'RMDA.CA', 'NIPH.CA', 'MPCI.CA', 'MCRO.CA', 'BIOC.CA',
    # Holdings
    'GBCO.CA', 'EKHO.CA', 'CCAP.CA', 'AIH.CA', 'AIDC.CA', 'ASPI.CA', 'VLMR.CA',
    # Transport
    'CSAG.CA', 'ETRS.CA', 'MPRC.CA',
]

# Sector mapping
SECTORS = {
    'COMI.CA': 'Banking', 'CIEB.CA': 'Banking', 'HDBK.CA': 'Banking', 'FAIT.CA': 'Banking',
    'ADIB.CA': 'Banking', 'SAUD.CA': 'Banking', 'EGBE.CA': 'Banking', 'EXPA.CA': 'Banking',
    'HRHO.CA': 'Financial', 'EFIH.CA': 'Financial', 'FWRY.CA': 'Financial', 'BTFH.CA': 'Financial', 'CICH.CA': 'Financial',
    'TMGH.CA': 'Real Estate', 'PHDC.CA': 'Real Estate', 'OCDI.CA': 'Real Estate', 'MASR.CA': 'Real Estate',
    'HELI.CA': 'Real Estate', 'EMFD.CA': 'Real Estate', 'AMER.CA': 'Real Estate', 'EHDR.CA': 'Real Estate',
    'ELKA.CA': 'Real Estate', 'ELSH.CA': 'Real Estate', 'ZMID.CA': 'Real Estate', 'EGTS.CA': 'Real Estate', 'MENA.CA': 'Real Estate',
    'ORAS.CA': 'Construction', 'ARCC.CA': 'Construction', 'SCEM.CA': 'Construction', 'MCQE.CA': 'Construction',
    'SVCE.CA': 'Construction', 'ENGC.CA': 'Construction', 'GGCC.CA': 'Construction',
    'SWDY.CA': 'Industrial', 'EGAL.CA': 'Industrial', 'ELEC.CA': 'Industrial', 'ORWE.CA': 'Industrial',
    'LCSW.CA': 'Industrial', 'ECAP.CA': 'Industrial', 'PRCL.CA': 'Industrial', 'CERA.CA': 'Industrial',
    'KABO.CA': 'Industrial', 'DSCW.CA': 'Industrial', 'MTIE.CA': 'Industrial',
    'ABUK.CA': 'Chemicals', 'MFPC.CA': 'Chemicals', 'SKPC.CA': 'Chemicals', 'AMOC.CA': 'Chemicals',
    'EGCH.CA': 'Chemicals', 'ICFC.CA': 'Chemicals', 'COSG.CA': 'Chemicals',
    'ASCM.CA': 'Oil & Gas', 'MOIL.CA': 'Oil & Gas', 'TAQA.CA': 'Oil & Gas',
    'ETEL.CA': 'Telecom', 'RAYA.CA': 'Telecom',
    'EAST.CA': 'Consumer', 'JUFO.CA': 'Consumer', 'EFID.CA': 'Consumer', 'POUL.CA': 'Consumer',
    'ISMA.CA': 'Consumer', 'MPCO.CA': 'Consumer', 'SUGR.CA': 'Consumer', 'IFAP.CA': 'Consumer',
    'ISPH.CA': 'Healthcare', 'CLHO.CA': 'Healthcare', 'PHAR.CA': 'Healthcare', 'RMDA.CA': 'Healthcare',
    'NIPH.CA': 'Healthcare', 'MPCI.CA': 'Healthcare', 'MCRO.CA': 'Healthcare', 'BIOC.CA': 'Healthcare',
    'GBCO.CA': 'Holdings', 'EKHO.CA': 'Holdings', 'CCAP.CA': 'Holdings', 'AIH.CA': 'Holdings',
    'AIDC.CA': 'Holdings', 'ASPI.CA': 'Holdings', 'VLMR.CA': 'Holdings',
    'CSAG.CA': 'Transport', 'ETRS.CA': 'Transport', 'MPRC.CA': 'Transport',
}

DATA_DIR = Path('/Users/seifhegazy/Documents/Grad Project/data/raw/stocks')
CSV_FILE = DATA_DIR / 'egx_daily_12y.csv'

def fetch_ticker_data(ticker: str, period: str = '12y') -> pd.DataFrame:
    """Fetch data for a single ticker."""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period)
        
        if df.empty:
            return pd.DataFrame()
        
        df = df.reset_index()
        df['Ticker'] = ticker
        df['Sector'] = SECTORS.get(ticker, 'Unknown')
        df = df.rename(columns={'Date': 'Date', 'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close', 'Volume': 'Volume'})
        df = df[['Date', 'Ticker', 'Sector', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        return df
    except Exception as e:
        print(f"  Error: {e}")
        return pd.DataFrame()

def main():
    print("="*60)
    print("EGX 100 STOCK DATA COLLECTION")
    print("="*60)
    print(f"\nTickers to fetch: {len(EGX100_TICKERS)}")
    print(f"Output file: {CSV_FILE}\n")
    
    # Load existing data
    if CSV_FILE.exists():
        existing = pd.read_csv(CSV_FILE)
        existing_tickers = set(existing['Ticker'].unique())
        print(f"Existing data: {len(existing)} rows, {len(existing_tickers)} tickers")
    else:
        existing = pd.DataFrame()
        existing_tickers = set()
    
    # Find new tickers
    new_tickers = [t for t in EGX100_TICKERS if t not in existing_tickers]
    print(f"New tickers to fetch: {len(new_tickers)}\n")
    
    if not new_tickers:
        print("All tickers already in database!")
        return
    
    # Fetch new tickers
    all_data = []
    success = 0
    failed = []
    
    for ticker in tqdm(new_tickers, desc="Fetching"):
        df = fetch_ticker_data(ticker)
        if not df.empty:
            all_data.append(df)
            success += 1
        else:
            failed.append(ticker)
    
    print(f"\n✓ Success: {success}/{len(new_tickers)}")
    if failed:
        print(f"✗ Failed: {failed}")
    
    # Append to existing data
    if all_data:
        new_df = pd.concat(all_data, ignore_index=True)
        print(f"\nNew data: {len(new_df)} rows")
        
        if not existing.empty:
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        
        # Sort and save
        combined = combined.sort_values(['Date', 'Ticker'])
        combined.to_csv(CSV_FILE, index=False)
        print(f"✓ Saved to {CSV_FILE}")
        print(f"  Total: {len(combined)} rows, {len(combined['Ticker'].unique())} tickers")

if __name__ == '__main__':
    main()
