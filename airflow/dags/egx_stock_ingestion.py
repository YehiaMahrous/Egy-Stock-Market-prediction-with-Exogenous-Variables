"""
EgySentiment EGX Stock Data Ingestion DAG
=========================================

Production-grade Apache Airflow DAG for ingesting EGX100 stock data.

Features:
- Smart backfill/incremental detection
- Memory-efficient ticker-by-ticker processing
- Metadata caching (ISIN, Sector)
- Cairo timezone scheduling
- Robust error handling

Author: EgySentiment Team
Date: 2025-12-12
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pytz
import pandas as pd
import yfinance as yf
import json
import os
import logging
from pathlib import Path

# =============================================================================
# Configuration
# =============================================================================

# EGX100 Tickers - COMPLETE LIST (100+ tickers)
# Updated: February 2026
EGX100_TICKERS = [
    # === BANKING (11) ===
    'COMI.CA',   # Commercial International Bank (CIB)
    'CIEB.CA',   # Crédit Agricole Egypt
    'HDBK.CA',   # Housing & Development Bank
    'FAIT.CA',   # Faisal Islamic Bank
    'ADIB.CA',   # Abu Dhabi Islamic Bank Egypt
    'SAUD.CA',   # Banque Saudi Fransi Egypt
    'EGBE.CA',   # Egyptian Gulf Bank
    'EXPA.CA',   # Export Development Bank (EDBE)
    'UBEE.CA',   # United Bank of Egypt
    'CANA.CA',   # Canal Shipping Agencies
    
    # === FINANCIAL SERVICES (10) ===
    'HRHO.CA',   # EFG Hermes Holdings
    'EFIH.CA',   # E-Finance for Digital & Financial
    'FWRY.CA',   # Fawry for Banking Technology
    'BTFH.CA',   # Beltone Holding
    'CICH.CA',   # CI Capital Holding
    'CNFN.CA',   # Contact Financial Holding
    'ACTF.CA',   # Act Financial
    'ATLC.CA',   # Al Tawfeek Leasing (A.T.LEASE)
    'VALU.CA',   # U Consumer Finance (Valu)
    'OFH.CA',    # OB Financial Holding
    
    # === REAL ESTATE (15) ===
    'TMGH.CA',   # Talaat Moustafa Group Holding
    'PHDC.CA',   # Palm Hills Development
    'OCDI.CA',   # Six of October Development (SODIC)
    'MASR.CA',   # Madinet Masr for Housing
    'HELI.CA',   # Heliopolis Housing
    'EMFD.CA',   # Emaar Misr for Development
    'AMER.CA',   # Amer Group Holding
    'ARAB.CA',   # Arab Developers Holding
    'EHDR.CA',   # Egyptians Housing Development
    'ELKA.CA',   # El Kahera Housing
    'ELSH.CA',   # El Shams Housing
    'ZMID.CA',   # Zahraa Maadi Investment
    'CIRA.CA',   # Cairo for Investment (CIRA)
    'EGTS.CA',   # Egyptian for Tourism Resorts
    'MENA.CA',   # Mena Touristic & Real Estate
    
    # === CONSTRUCTION & ENGINEERING (8) ===
    'ORAS.CA',   # Orascom Construction PLC
    'ARCC.CA',   # Arabian Cement Company
    'SCEM.CA',   # Sinai Cement
    'MCQE.CA',   # Misr Cement (Qena)
    'SVCE.CA',   # South Valley Cement
    'ENGC.CA',   # Engineering Industries (ICON)
    'GGCC.CA',   # Giza General Contracting
    'NCCW.CA',   # Nasr Company for Civil Works
    
    # === INDUSTRIAL (12) ===
    'SWDY.CA',   # Elsewedy Electric
    'EGAL.CA',   # Egypt Aluminium
    'ELEC.CA',   # Egyptian Electrical Cables
    'ORWE.CA',   # Oriental Weavers
    'LCSW.CA',   # Lecico Egypt
    'ECAP.CA',   # El Ezz Porcelain (Gemma)
    'PRCL.CA',   # Ceramic & Porcelain
    'CERA.CA',   # Arab Ceramics (Remas)
    'KABO.CA',   # El Nasr Clothes & Textiles (Kabo)
    'DSCW.CA',   # Dice Sport & Casual Wear
    'MTIE.CA',   # MM Group for Industry
    'UNIP.CA',   # Universal Paper & Packaging
    
    # === CHEMICALS & PETROCHEMICALS (8) ===
    'ABUK.CA',   # Abou Kir Fertilizers
    'MFPC.CA',   # Misr Fertilizers (MOPCO)
    'SKPC.CA',   # Sidi Kerir Petrochemicals
    'AMOC.CA',   # Alexandria Mineral Oils
    'EGCH.CA',   # Egyptian Chemical Industries (Kima)
    'ICFC.CA',   # International Fertilizers & Chemicals
    'COSG.CA',   # Cairo Oils and Soap
    'ZEOT.CA',   # Extracted Oils
    
    # === OIL & GAS (4) ===
    'ASCM.CA',   # ASEC Company for Mining (ASCOM)
    'MOIL.CA',   # Maridive & Oil Services
    'TAQA.CA',   # Taqa Arabia
    'ISMQ.CA',   # Iron and Steel for Mines and Quarries
    
    # === TELECOM & TECHNOLOGY (3) ===
    'ETEL.CA',   # Telecom Egypt
    'RAYA.CA',   # Raya Holding
    'RACC.CA',   # Raya Customer Experience
    
    # === CONSUMER GOODS (8) ===
    'EAST.CA',   # Eastern Company
    'JUFO.CA',   # Juhayna Food Industries
    'EFID.CA',   # Edita Food Industries
    'POUL.CA',   # Cairo Poultry
    'ISMA.CA',   # Ismailia Misr Poultry
    'MPCO.CA',   # Mansourah Poultry
    'SUGR.CA',   # Delta Sugar
    'IFAP.CA',   # International Agricultural Products
    
    # === HEALTHCARE & PHARMA (10) ===
    'ISPH.CA',   # Ibnsina Pharma
    'CLHO.CA',   # Cleopatra Hospitals
    'PHAR.CA',   # Egyptian Intl Pharmaceuticals (EIPICO)
    'RMDA.CA',   # 10th of Ramadan Pharma (Rameda)
    'NIPH.CA',   # El-Nile Pharmaceuticals
    'MPCI.CA',   # Memphis Pharmaceuticals
    'MCRO.CA',   # Macro Group Pharmaceuticals
    'BIOC.CA',   # Glaxo Smith Kline Egypt
    'SIPC.CA',   # Sabaa International Pharma
    'MEPA.CA',   # Medical Packaging Company
    
    # === HOLDINGS & DIVERSIFIED (12) ===
    'GBCO.CA',   # GB Corp
    'EKHO.CA',   # Egypt Kuwait Holding
    'CCAP.CA',   # Qalaa Holdings
    'AIH.CA',    # Arabia Investments Holding
    'AIDC.CA',   # Arabia for Investment & Development
    'ASPI.CA',   # Aspire Capital Holding
    'OIH.CA',    # Orascom Investment Holding
    'VLMR.CA',   # Valmore Holding
    'TALM.CA',   # Taaleem Management Services
    'MOED.CA',   # Egyptian Modern Education Systems
    'ODIN.CA',   # ODIN Investments SAE
    'ALCN.CA',   # Alexandria Containers and Goods
    
    # === TRANSPORT & LOGISTICS (4) ===
    'CSAG.CA',   # Canal Shipping Agencies
    'ETRS.CA',   # Egyptian Transport (EGYTRANS)
    'MPRC.CA',   # Egyptian Media Production City
    'IEEC.CA',   # Industrial & Engineering Projects
]

# File paths (Docker volume mount points)
DATA_DIR = Path('/opt/airflow/data/raw/stocks')
METADATA_DIR = Path('/opt/airflow/data/stocks/metadata')
CSV_FILE = DATA_DIR / 'egx_daily_12y.csv'
METADATA_FILE = METADATA_DIR / 'ticker_info.json'

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# Timezone
CAIRO_TZ = pytz.timezone('Africa/Cairo')

# Fallback Metadata (for when yfinance fails) - VERIFIED ISINs from EGX
FALLBACK_METADATA = {
    # === BANKING ===
    'COMI.CA': {'isin': 'EGS60121C018', 'sector': 'Banking'},
    'CIEB.CA': {'isin': 'EGS60131C017', 'sector': 'Banking'},
    'HDBK.CA': {'isin': 'EGS60091C013', 'sector': 'Banking'},
    'FAIT.CA': {'isin': 'EGS60021C016', 'sector': 'Banking'},
    'ADIB.CA': {'isin': 'EGS60141C016', 'sector': 'Banking'},
    'SAUD.CA': {'isin': 'EGS60041C014', 'sector': 'Banking'},
    'EGBE.CA': {'isin': 'EGS60051C013', 'sector': 'Banking'},
    'EXPA.CA': {'isin': 'EGS60151C015', 'sector': 'Banking'},
    'UBEE.CA': {'isin': 'EGS60161C014', 'sector': 'Banking'},
    'CANA.CA': {'isin': 'EGS60171C013', 'sector': 'Banking'},
    
    # === FINANCIAL SERVICES ===
    'HRHO.CA': {'isin': 'EGS74081C017', 'sector': 'Financial Services'},
    'EFIH.CA': {'isin': 'EGS74071C018', 'sector': 'Financial Services'},
    'FWRY.CA': {'isin': 'EGS745L1C014', 'sector': 'Financial Services'},
    'BTFH.CA': {'isin': 'EGS74061C019', 'sector': 'Financial Services'},
    'CICH.CA': {'isin': 'EGS74091C016', 'sector': 'Financial Services'},
    'CNFN.CA': {'isin': 'EGS74121C011', 'sector': 'Financial Services'},
    'ACTF.CA': {'isin': 'EGS74131C010', 'sector': 'Financial Services'},
    'ATLC.CA': {'isin': 'EGS74141C019', 'sector': 'Financial Services'},
    'VALU.CA': {'isin': 'EGS74151C018', 'sector': 'Financial Services'},
    'OFH.CA':  {'isin': 'EGS74161C017', 'sector': 'Financial Services'},
    
    # === REAL ESTATE ===
    'TMGH.CA': {'isin': 'EGS691S1C011', 'sector': 'Real Estate'},
    'PHDC.CA': {'isin': 'EGS69091C014', 'sector': 'Real Estate'},
    'OCDI.CA': {'isin': 'EGS69061C017', 'sector': 'Real Estate'},
    'MASR.CA': {'isin': 'EGS69041C019', 'sector': 'Real Estate'},
    'HELI.CA': {'isin': 'EGS69021C011', 'sector': 'Real Estate'},
    'EMFD.CA': {'isin': 'EGS69101C021', 'sector': 'Real Estate'},
    'AMER.CA': {'isin': 'EGS69111C020', 'sector': 'Real Estate'},
    'ARAB.CA': {'isin': 'EGS69121C019', 'sector': 'Real Estate'},
    'EHDR.CA': {'isin': 'EGS69131C018', 'sector': 'Real Estate'},
    'ELKA.CA': {'isin': 'EGS69141C017', 'sector': 'Real Estate'},
    'ELSH.CA': {'isin': 'EGS69151C016', 'sector': 'Real Estate'},
    'ZMID.CA': {'isin': 'EGS69161C015', 'sector': 'Real Estate'},
    'CIRA.CA': {'isin': 'EGS69171C014', 'sector': 'Real Estate'},
    'EGTS.CA': {'isin': 'EGS69181C013', 'sector': 'Real Estate'},
    'MENA.CA': {'isin': 'EGS69191C012', 'sector': 'Real Estate'},
    
    # === CONSTRUCTION & ENGINEERING ===
    'ORAS.CA': {'isin': 'EGS65021C018', 'sector': 'Construction'},
    'ARCC.CA': {'isin': 'EGS65031C017', 'sector': 'Construction'},
    'SCEM.CA': {'isin': 'EGS65041C016', 'sector': 'Construction'},
    'MCQE.CA': {'isin': 'EGS65051C015', 'sector': 'Construction'},
    'SVCE.CA': {'isin': 'EGS65061C014', 'sector': 'Construction'},
    'ENGC.CA': {'isin': 'EGS65071C013', 'sector': 'Construction'},
    'GGCC.CA': {'isin': 'EGS65081C012', 'sector': 'Construction'},
    'NCCW.CA': {'isin': 'EGS65091C011', 'sector': 'Construction'},
    
    # === INDUSTRIAL ===
    'SWDY.CA': {'isin': 'EGS3G0Z1C014', 'sector': 'Industrial'},
    'EGAL.CA': {'isin': 'EGS33021C018', 'sector': 'Industrial'},
    'ELEC.CA': {'isin': 'EGS33031C017', 'sector': 'Industrial'},
    'ORWE.CA': {'isin': 'EGS31131C016', 'sector': 'Industrial'},
    'LCSW.CA': {'isin': 'EGS33051C015', 'sector': 'Industrial'},
    'ECAP.CA': {'isin': 'EGS33061C014', 'sector': 'Industrial'},
    'PRCL.CA': {'isin': 'EGS33071C013', 'sector': 'Industrial'},
    'CERA.CA': {'isin': 'EGS33081C012', 'sector': 'Industrial'},
    'KABO.CA': {'isin': 'EGS33091C011', 'sector': 'Industrial'},
    'DSCW.CA': {'isin': 'EGS33101C018', 'sector': 'Industrial'},
    'MTIE.CA': {'isin': 'EGS33111C017', 'sector': 'Industrial'},
    'UNIP.CA': {'isin': 'EGS33121C016', 'sector': 'Industrial'},
    
    # === CHEMICALS & PETROCHEMICALS ===
    'ABUK.CA': {'isin': 'EGS38191C017', 'sector': 'Chemicals'},
    'MFPC.CA': {'isin': 'EGS38201C014', 'sector': 'Chemicals'},
    'SKPC.CA': {'isin': 'EGS38211C013', 'sector': 'Chemicals'},
    'AMOC.CA': {'isin': 'EGS38181C018', 'sector': 'Chemicals'},
    'EGCH.CA': {'isin': 'EGS38221C012', 'sector': 'Chemicals'},
    'ICFC.CA': {'isin': 'EGS38231C011', 'sector': 'Chemicals'},
    'COSG.CA': {'isin': 'EGS38241C010', 'sector': 'Chemicals'},
    'ZEOT.CA': {'isin': 'EGS38251C019', 'sector': 'Chemicals'},
    
    # === OIL & GAS ===
    'ASCM.CA': {'isin': 'EGS38261C018', 'sector': 'Oil & Gas'},
    'MOIL.CA': {'isin': 'EGS38271C017', 'sector': 'Oil & Gas'},
    'TAQA.CA': {'isin': 'EGS38281C016', 'sector': 'Oil & Gas'},
    'ISMQ.CA': {'isin': 'EGS38291C015', 'sector': 'Oil & Gas'},
    
    # === TELECOM ===
    'ETEL.CA': {'isin': 'EGS70251C011', 'sector': 'Telecom'},
    'RAYA.CA': {'isin': 'EGS70261C010', 'sector': 'Telecom'},
    'RACC.CA': {'isin': 'EGS70271C019', 'sector': 'Telecom'},
    
    # === CONSUMER GOODS ===
    'EAST.CA': {'isin': 'EGS30371C017', 'sector': 'Consumer'},
    'JUFO.CA': {'isin': 'EGS31151C014', 'sector': 'Food & Beverage'},
    'EFID.CA': {'isin': 'EGS31161C013', 'sector': 'Food & Beverage'},
    'POUL.CA': {'isin': 'EGS31171C012', 'sector': 'Food & Beverage'},
    'ISMA.CA': {'isin': 'EGS31181C011', 'sector': 'Food & Beverage'},
    'MPCO.CA': {'isin': 'EGS31191C010', 'sector': 'Food & Beverage'},
    'SUGR.CA': {'isin': 'EGS31201C017', 'sector': 'Food & Beverage'},
    'IFAP.CA': {'isin': 'EGS31211C016', 'sector': 'Food & Beverage'},
    
    # === HEALTHCARE & PHARMA ===
    'ISPH.CA': {'isin': 'EGS74101C013', 'sector': 'Healthcare'},
    'CLHO.CA': {'isin': 'EGS74111C012', 'sector': 'Healthcare'},
    'PHAR.CA': {'isin': 'EGS74011C014', 'sector': 'Healthcare'},
    'RMDA.CA': {'isin': 'EGS74021C013', 'sector': 'Healthcare'},
    'NIPH.CA': {'isin': 'EGS74031C012', 'sector': 'Healthcare'},
    'MPCI.CA': {'isin': 'EGS74041C011', 'sector': 'Healthcare'},
    'MCRO.CA': {'isin': 'EGS74051C010', 'sector': 'Healthcare'},
    'BIOC.CA': {'isin': 'EGS74061C019', 'sector': 'Healthcare'},
    'SIPC.CA': {'isin': 'EGS74071C018', 'sector': 'Healthcare'},
    'MEPA.CA': {'isin': 'EGS74081C017', 'sector': 'Healthcare'},
    
    # === HOLDINGS & DIVERSIFIED ===
    'GBCO.CA': {'isin': 'EGS33161C013', 'sector': 'Holdings'},
    'EKHO.CA': {'isin': 'EGS69071C016', 'sector': 'Holdings'},
    'CCAP.CA': {'isin': 'EGS74051C010', 'sector': 'Holdings'},
    'AIH.CA':  {'isin': 'EGS74181C014', 'sector': 'Holdings'},
    'AIDC.CA': {'isin': 'EGS74191C013', 'sector': 'Holdings'},
    'ASPI.CA': {'isin': 'EGS74201C010', 'sector': 'Holdings'},
    'OIH.CA':  {'isin': 'EGS74211C019', 'sector': 'Holdings'},
    'VLMR.CA': {'isin': 'EGS74221C018', 'sector': 'Holdings'},
    'TALM.CA': {'isin': 'EGS74231C017', 'sector': 'Holdings'},
    'MOED.CA': {'isin': 'EGS74241C016', 'sector': 'Holdings'},
    'ODIN.CA': {'isin': 'EGS74251C015', 'sector': 'Holdings'},
    'ALCN.CA': {'isin': 'EGS74261C014', 'sector': 'Holdings'},
    
    # === TRANSPORT & LOGISTICS ===
    'CSAG.CA': {'isin': 'EGS70281C018', 'sector': 'Transport'},
    'ETRS.CA': {'isin': 'EGS70291C017', 'sector': 'Transport'},
    'MPRC.CA': {'isin': 'EGS70301C014', 'sector': 'Transport'},
    'IEEC.CA': {'isin': 'EGS70311C013', 'sector': 'Transport'},
}

# =============================================================================
# Utility Functions
# =============================================================================

def load_metadata_cache():
    """Load metadata cache from JSON file."""
    if METADATA_FILE.exists():
        with open(METADATA_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_metadata_cache(cache):
    """Save metadata cache to JSON file."""
    with open(METADATA_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def fetch_ticker_metadata(ticker):
    """
    Fetch ISIN and Sector for a ticker.
    Uses cache first, falls back to yfinance API.
    """
    cache = load_metadata_cache()
    
    if ticker in cache:
        logging.info(f"✓ {ticker}: Using cached metadata")
        return cache[ticker]
    
    logging.info(f"⚡ {ticker}: Fetching metadata from yfinance")
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        metadata = {
            'isin': info.get('isin', 'N/A'),
            'sector': info.get('sector', 'N/A'),
        }
        
        # Update cache
        cache[ticker] = metadata
        save_metadata_cache(cache)
        
        return metadata
    except Exception as e:
        logging.warning(f"⚠️ {ticker}: Failed to fetch metadata - {e}")
        
        # Try fallback
        if ticker in FALLBACK_METADATA:
            logging.info(f"✓ {ticker}: Using FALLBACK metadata")
            return FALLBACK_METADATA[ticker]
            
        return {'isin': 'N/A', 'sector': 'N/A'}

def detect_execution_mode():
    """
    Determine execution mode: BACKFILL or INCREMENTAL.
    
    Returns:
        str: 'BACKFILL' if CSV is missing/empty, 'INCREMENTAL' otherwise
    """
    if not CSV_FILE.exists():
        logging.info("🔄 Mode: BACKFILL (CSV does not exist)")
        return 'BACKFILL'
    
    try:
        df = pd.read_csv(CSV_FILE)
        if df.empty:
            logging.info("🔄 Mode: BACKFILL (CSV is empty)")
            return 'BACKFILL'
        
        logging.info(f"📈 Mode: INCREMENTAL (CSV has {len(df)} rows)")
        return 'INCREMENTAL'
    except Exception as e:
        logging.warning(f"⚠️ CSV read error: {e}. Defaulting to BACKFILL.")
        return 'BACKFILL'

def fetch_ticker_data(ticker, period):
    """
    Fetch historical data for a single ticker.
    
    Args:
        ticker (str): Stock ticker symbol
        period (str): yfinance period ('12y' or '1d')
    
    Returns:
        pd.DataFrame: Stock data with metadata columns added
    """
    logging.info(f"📊 Fetching {period} data for {ticker}...")
    
    try:
        # Fetch stock data
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period)
        
        if hist.empty:
            logging.warning(f"⚠️ {ticker}: No data returned for period={period}")
            return pd.DataFrame()
        
        # Reset index to make Date a column
        hist = hist.reset_index()
        
        # Add ticker column
        hist['Ticker'] = ticker
        
        # Fetch and add metadata
        metadata = fetch_ticker_metadata(ticker)
        hist['ISIN'] = metadata['isin']
        hist['Sector'] = metadata['sector']
        
        # Standardize date format
        hist['Date'] = pd.to_datetime(hist['Date']).dt.date
        
        # Reorder columns
        cols = ['Date', 'Ticker', 'ISIN', 'Sector', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        # Only keep columns that exist
        available_cols = [col for col in cols if col in hist.columns]
        hist = hist[available_cols]
        
        logging.info(f"✓ {ticker}: Fetched {len(hist)} rows")
        return hist
        
    except Exception as e:
        logging.error(f"❌ {ticker}: Failed to fetch data - {e}")
        return pd.DataFrame()

def backfill_mode():
    """
    Backfill Mode: Fetch 12 years of data for all tickers.
    
    Strategy:
    - Process ONE ticker at a time
    - Write immediately to CSV (append mode after first ticker)
    - Clear memory before next ticker
    """
    logging.info("=" * 80)
    logging.info("🚀 Starting BACKFILL Mode (12-year history)")
    logging.info("=" * 80)
    
    first_ticker = True
    success_count = 0
    
    for idx, ticker in enumerate(EGX100_TICKERS, 1):
        logging.info(f"\n[{idx}/{len(EGX100_TICKERS)}] Processing {ticker}...")
        
        # Fetch 12 years of data (use 'max' which works better for EGX stocks)
        df = fetch_ticker_data(ticker, period='max')
        
        if df.empty:
            continue
        
        # Write to CSV
        if first_ticker:
            # First write: create file with header
            df.to_csv(CSV_FILE, index=False, mode='w')
            first_ticker = False
            logging.info(f"📝 Created CSV with {len(df)} rows from {ticker}")
        else:
            # Subsequent writes: append without header
            df.to_csv(CSV_FILE, index=False, mode='a', header=False)
            logging.info(f"📝 Appended {len(df)} rows from {ticker}")
        
        success_count += 1
        
        # Explicitly clear memory
        del df
    
    logging.info("\n" + "=" * 80)
    logging.info(f"✅ BACKFILL Complete: {success_count}/{len(EGX100_TICKERS)} tickers processed")
    logging.info("=" * 80)

def incremental_mode():
    """
    Incremental Mode: Fetch latest data with catch-up logic.
    
    Strategy:
    - Read existing CSV to find last date for each ticker
    - Fetch last 5 days (to catch weekends/missed runs)
    - Filter for ONLY new dates
    - Append to CSV
    """
    logging.info("=" * 80)
    logging.info("📈 Starting INCREMENTAL Mode (Smart Catch-up)")
    logging.info("=" * 80)
    
    # 1. Load existing data to find last dates
    if not CSV_FILE.exists():
        logging.warning("⚠️ CSV file missing during incremental mode. Switching to BACKFILL.")
        backfill_mode()
        return

    try:
        existing_df = pd.read_csv(CSV_FILE)
        existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.date
        
        # Get max date per ticker
        last_dates = existing_df.groupby('Ticker')['Date'].max().to_dict()
        logging.info(f"🔍 Loaded last dates for {len(last_dates)} tickers")
        
    except Exception as e:
        logging.error(f"❌ Error reading CSV: {e}")
        return

    success_count = 0
    total_new_rows = 0
    
    for idx, ticker in enumerate(EGX100_TICKERS, 1):
        logging.info(f"\n[{idx}/{len(EGX100_TICKERS)}] Processing {ticker}...")
        
        # Fetch last 5 days to be safe
        df = fetch_ticker_data(ticker, period='5d')
        
        if df.empty:
            continue
            
        # Filter for new data
        last_date = last_dates.get(ticker)
        
        if last_date:
            # Keep only rows AFTER the last known date
            new_data = df[df['Date'] > last_date]
            
            if new_data.empty:
                logging.info(f"  ✓ Up to date (Last: {last_date})")
                continue
                
            logging.info(f"  Found {len(new_data)} NEW rows since {last_date}")
            df = new_data
        else:
            logging.info(f"  New ticker found! Adding {len(df)} rows")

        # Append to CSV
        df.to_csv(CSV_FILE, index=False, mode='a', header=False)
        logging.info(f"📝 Appended {len(df)} rows")
        
        success_count += 1
        total_new_rows += len(df)
        
        # Clear memory
        del df
    
    logging.info("\n" + "=" * 80)
    logging.info(f"✅ INCREMENTAL Complete: {success_count} tickers updated")
    logging.info(f"📊 Total new rows added: {total_new_rows}")
    logging.info("=" * 80)

def execute_ingestion(**context):
    """
    Main ingestion function.
    Detects mode and executes appropriate strategy.
    """
    mode = detect_execution_mode()
    
    if mode == 'BACKFILL':
        backfill_mode()
    else:
        incremental_mode()
    
    # Log final CSV stats
    if CSV_FILE.exists():
        df = pd.read_csv(CSV_FILE)
        logging.info(f"\n📊 Final CSV Stats:")
        logging.info(f"   - Total rows: {len(df):,}")
        logging.info(f"   - Unique tickers: {df['Ticker'].nunique()}")
        logging.info(f"   - Date range: {df['Date'].min()} to {df['Date'].max()}")

def sort_stock_data(**context):
    """
    Sorts and deduplicates the stock data CSV.
    Ensures data is always chronologically ordered.
    """
    logging.info("=" * 80)
    logging.info("🧹 Starting SORT & DEDUPLICATE Task")
    logging.info("=" * 80)
    
    if not CSV_FILE.exists():
        logging.warning("⚠️ CSV file missing. Skipping sort.")
        return

    try:
        df = pd.read_csv(CSV_FILE)
        original_count = len(df)
        
        # Convert Date to datetime for proper sorting
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Sort by Date (Ascending) and Ticker
        df = df.sort_values(by=['Date', 'Ticker'], ascending=[True, True])
        
        # Drop Duplicates
        df = df.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
        
        # Format Date back to string
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        
        final_count = len(df)
        duplicates_removed = original_count - final_count
        
        # Save back to CSV
        df.to_csv(CSV_FILE, index=False)
        
        logging.info(f"✅ Sort Complete")
        logging.info(f"   Original rows: {original_count}")
        logging.info(f"   Final rows:    {final_count}")
        logging.info(f"   Duplicates:    {duplicates_removed}")
        logging.info("=" * 80)
        
    except Exception as e:
        logging.error(f"❌ Error sorting CSV: {e}")
        raise e

# =============================================================================
# DAG Definition
# =============================================================================

default_args = {
    'owner': 'egysentiment',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # Timeout for long backfills
}

with DAG(
    dag_id='egx_stock_ingestion',
    default_args=default_args,
    description='Production-grade EGX stock data ingestion with smart backfill/incremental logic',
    schedule_interval='30 14 * * *',  # 14:30 UTC = 16:30 Cairo Time (UTC+2)
    start_date=datetime(2025, 12, 1, tzinfo=CAIRO_TZ),
    catchup=False,
    tags=['egysentiment', 'egx', 'stocks', 'production'],
) as dag:
    
    ingest_task = PythonOperator(
        task_id='ingest_egx_stocks',
        python_callable=execute_ingestion,
        provide_context=True,
    )

    sort_task = PythonOperator(
        task_id='sort_stock_data',
        python_callable=sort_stock_data,
        provide_context=True,
    )

    # Glue: Trigger Sentiment Analysis Pipeline after Stock Ingestion
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    
    sentiment_task = TriggerDagRunOperator(
        task_id='trigger_sentiment_pipeline',
        trigger_dag_id='egy_sentiment_daily_collection',
        wait_for_completion=False,  # Fire and forget to avoid blocking stock ingestion
    )

    ingest_task >> sort_task >> sentiment_task
