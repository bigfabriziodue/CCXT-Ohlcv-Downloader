import ccxt
import pandas as pd
import os
import time
import sys
import msvcrt
import shutil
import ctypes
import argparse
import glob
import threading
from datetime import datetime, timezone
from typing import List
from datetime import timedelta
from threading import Thread
from typing import Optional

# ────────────────────────────────────────────────────────────────────────────────
# CONFIGURAZIONE MODIFICABILE
# ────────────────────────────────────────────────────────────────────────────────
TIMEFRAME = "1m"                   #Timeframe delle candele. Valori: 1m;5m;15m;30m;1h;4h;1d;
START_DATE = "2021-12-31 23:00:00" #Data di inizio in formato UTC

BINANCE_SYMBOLS: List[str] = [        #Simboli da scaricare usando Binance
    "BTC/EUR",
    "BAT/BTC",
]

CRYPTOCOM_SYMBOLS: List[str] = [         #Simboli da scaricare usando Crypto.com

]

BATCH_LIMIT       = 1000  #Dimensione massima per fetch_ohlcv
PARTIAL_EVERY     = 10    #Ogni N batch salva un CSV parziale
MAX_RETRIES       = 3     #Numero massimo di tentativi per lo stesso errore
RATE_LIMIT_SAFETY = 1.10  #Aggiunge il 10 % al rateLimit dell'exchange
DEBUG = True              #Attiva o no la visualizzazione dei messaggi di DEBUG
BREAKPOINT = False         #Attiva o no lo stop ai breakpoint
DEBUGDF = True           #Attiva o no l'output del Dataframe a ogni operazione
LOG = True                #Attiva o no il Debug su File
SHOWCONSOLE = False
# ────────────────────────────────────────────────────────────────────────────────

# ────────────────────────────────────────────────────────────────────────────────
# FUNZIONE DI PRINT DEBUG
# ────────────────────────────────────────────────────────────────────────────────
def debug_print(*args, level="d", **kwargs):
    if DEBUG:
        prefixes = {
            "d": "[DEBUG]:",
            "w": "[WARNING]:",
            "e": "[ERROR]:",
        }
        prefix = prefixes.get(level.lower(), "[DEBUG]:")
        if (SHOWCONSOLE): print(prefix, *args, **kwargs)
        msg = " ".join(str(a) for a in args)
        if LOG:
            with open("log.txt", "a", encoding="utf-8") as f:
                f.write(f"{prefix} {msg}\n")
                f.flush()                  # forza flush
                os.fsync(f.fileno())       # forza scrittura su disco

# ────────────────────────────────────────────────────────────────────────────────
# ABILITA SEQUENZE ANSI SU WINDOWS (per riscrivere più righe senza scroll)
# ────────────────────────────────────────────────────────────────────────────────
if os.name == "nt":
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE = -11
    mode = ctypes.c_ulong()
    kernel32.GetConsoleMode(handle, ctypes.byref(mode))
    # ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
    kernel32.SetConsoleMode(handle, mode.value | 0x0004)
    
# ────────────────────────────────────────────────────────────────────────────────
# Configurazione e calcoli iniziali
# ────────────────────────────────────────────────────────────────────────────────
start_ts_ms = int(datetime.fromisoformat(START_DATE)
                  .replace(tzinfo=timezone.utc).timestamp() * 1000)

timeframe_to_milliseconds = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

timeframe_ms = timeframe_to_milliseconds.get(TIMEFRAME)


binance = ccxt.binance({"enableRateLimit": True})
cryptocom  = ccxt.cryptocom({"enableRateLimit": True})
exchanges = {binance: BINANCE_SYMBOLS, cryptocom: CRYPTOCOM_SYMBOLS}

total_pairs = sum(len(syms) for syms in exchanges.values())
current_pair_idx = 0
stop_flag = False
printed_lines = 0
skipanswer = False
last_symbol: str | None = None
last_timestamp_str: str | None = None
btc_df= None
btc_df_lock = threading.Lock()
btc_price_dict = {}
# ────────────────────────────────────────────────────────────────────────────────
# Output multilinea che si sovrascrive (niente scroll)
# ────────────────────────────────────────────────────────────────────────────────
def progress_print(lines: List[str]) -> None:
    """Stampa una lista di linee riscrivendole in posizione fissa (ANSI)."""
    global printed_lines
    cols = shutil.get_terminal_size((120, 20)).columns
    if printed_lines:
        sys.stdout.write(f"\x1b[{printed_lines}F")  # Cursor Up
    sys.stdout.write("\x1b[J")  # Clear screen from cursor down
    for ln in lines:
        #ln = ln[:cols - 1] #Elimina le lettere che supera la lunghezza massima della finestra
        print(ln.ljust(cols))
    printed_lines = len(lines)
    sys.stdout.flush()

# ────────────────────────────────────────────────────────────────────────────────
# Controllo Ctrl+E (Windows) per interrompere
# ────────────────────────────────────────────────────────────────────────────────
def check_escape() -> None:
    global stop_flag
    while msvcrt.kbhit():
        if msvcrt.getch() == b"\x05":  # Ctrl+E
            stop_flag = True
# ────────────────────────────────────────────────────────────────────────────────
# Funzione di caricamento CSV BTC in Dataframe
# ────────────────────────────────────────────────────────────────────────────────
def load_btc_csv():
    debug_print("Thread Load CSV BTC avviato!")
    try:
        global btc_df, btc_price_dict
        path = os.path.join(os.path.dirname(__file__), "BTC.csv")
        df = pd.read_csv(path, sep=";", dtype={"timestamp": str})
        with btc_df_lock:
            btc_df = df
            btc_price_dict = btc_df.set_index("timestamp")["close"].to_dict()
        debug_print("CSV BTC caricato")
    except Exception as e:
        debug_print("Non sono riuscito a caricare il CSV di BTC:\n" + str(e), level="w")

load_btc_csv_thread = Thread(target = load_btc_csv)
# ────────────────────────────────────────────────────────────────────────────────
# Utility lettura/scrittura CSV
# ────────────────────────────────────────────────────────────────────────────────
def load_csv_safe(filename: str) -> pd.DataFrame:
    if not os.path.exists(filename):
        debug_print("Non ho trovato un csv completo.")
        return pd.DataFrame()
    try:
        debug_print("Ho trovato un csv completo.")
        df = pd.read_csv(filename, sep=";")
        df.columns = [c.lower() for c in df.columns]
        if "timestamp" in df.columns and not df.empty:
            return df
        elif df.empty:
            raise ValueError("CSV senza dati.")
        else:
            raise ValueError("CSV non formattato corretamente.")
    except Exception as e:
        debug_print("Non sono riuscito a decodificare il csv completo:\n" + str(e), level="w")
        return pd.DataFrame()

def save_partial(buffer: list, partial_csv: str) -> None:
    if not buffer:
        debug_print("Il buffer è vuoto, non c'è nulla da salvare.", level="w")
        return
    df = pd.DataFrame(buffer, columns=["timestamp", "close"])
    csv_exists = os.path.exists(partial_csv)
    mode = "a" if csv_exists else "w"
    header = not csv_exists
    if DEBUG: debug_print(f"Salvo {len(buffer)} righe in mode {mode} con header {header}.")
    if DEBUGDF: debug_print (f"{df}")
    df.to_csv(partial_csv, mode=mode, header=header, index=False, sep=";")
    
# ────────────────────────────────────────────────────────────────────────────────
# Consolidamento: parziale → completo
# ────────────────────────────────────────────────────────────────────────────────
def get_btc_price(timestamp: str) -> Optional[float]:
    with btc_df_lock:
        if btc_df is None or btc_df.empty:
            debug_print ("Dataframe BTC/EUR non caricato correttamente!", level="e")
            print ("Dataframe BTC/EUR non caricato correttamente. Uscita...")
            sys.exit(1)
    #row = btc_df[btc_df["timestamp"] == timestamp]
    if (BREAKPOINT): input("Breakpoint")
    price_str = btc_price_dict.get(timestamp)
    debug_print(f"Timestamp in get_btc_price: {timestamp}")
    if price_str is not None:
    #if not row.empty:
        #btc_price = row.iloc[0]["close"]
        #return float(str(btc_price).replace(",", "."))
        return float(str(price_str).replace(",", "."))
    else:
        debug_print(f"Timestamp BTC non trovato: {timestamp}", level="w")
        return None

def consolidate_csv(full_csv: str, partial_csv: str, convert_btc: bool) -> None:
    print(f"Consolidamento in corso: {partial_csv} → {full_csv}...")

    frames: list[pd.DataFrame] = []
    if os.path.exists(full_csv):
        os.remove(full_csv)
        if DEBUG: debug_print(f"Rimosso csv completo trovato in precedenza: {full_csv}.")

    if os.path.exists(partial_csv):
        df_partial = pd.read_csv(partial_csv, sep=";")
        df_partial.columns = [c.lower() for c in df_partial.columns]
        frames.append(df_partial)
        debug_print(f"Aggiornato csv parziale: {partial_csv}.")

    if not frames:
        debug_print("Il buffer è vuoto. Niente da consolidare.", level="e")
        return

    df_all = pd.concat(frames, ignore_index=True).drop_duplicates(subset="timestamp")

#    def parse_timestamp(ts) -> Union[datetime, NaTType]:
#        try:
#            if isinstance(ts, (int, float)):
#                return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
#            return datetime.strptime(str(ts), "%d/%m/%Y %H:%M")
#        except Exception as e:
#            debug_print(f"Timestamp non valido durante il parsing: {ts} - {e}", level="w")
#            return pd.NaT

#     df_all["timestamp"] = df_all["timestamp"].apply(parse_timestamp)

    if (DEBUGDF): debug_print(df_all.head(3))
    
    df_all["timestamp"] = pd.to_datetime(df_all["timestamp"], unit="ms", utc=True)
    
    if (DEBUGDF): debug_print("Dopo timestamp to datetime",df_all.head(3))
    
    df_all.dropna(subset=["timestamp"], inplace=True)
    df_all.sort_values("timestamp", inplace=True)
    df_all["timestamp"] = df_all["timestamp"].dt.strftime("%d/%m/%Y %H:%M")
    debug_print("Formattato correttamente tutti i timestamp.")
    if (BREAKPOINT): input("Breakpoint")
          

    def format_close(val, timestamp):
        try:
            price = float(val)
            if convert_btc and load_btc_csv_thread:
                if load_btc_csv_thread.is_alive():
                    debug_print("Attendo Thread...") 
                    load_btc_csv_thread.join()
                btc_price = get_btc_price(timestamp)
                if btc_price is None: return None
                price *= btc_price
            return f"{price:.2f}".replace(".", ",")
        except Exception as e:
            debug_print(f"Non sono riuscito a formattare {val} in {timestamp}:\n" + str(e), level="w")
            return None

    df_all["close"] = df_all.apply(lambda row: format_close(row["close"], row["timestamp"]), axis=1) # type: ignore
    df_all.dropna(subset=['close'], inplace=True)
    if (DEBUGDF): debug_print("Dopo format_close",df_all.head(3))
    debug_print("Formattato e convertiti correttamente tutti i prezzi.")

    df_all.to_csv(full_csv, index=False, sep=";")

    if os.path.exists(partial_csv):
        os.remove(partial_csv)

# ────────────────────────────────────────────────────────────────────────────────
# Conversione: completo → parziale
# ────────────────────────────────────────────────────────────────────────────────

def full_to_partial_conversion(full_csv: str, partial_csv: str) -> int:
    try:
        print(f"Conversione {full_csv} → {partial_csv} (ripartenza)...")
        if (DEBUG): debug_print(f"{full_csv} csv caricato su check convert_btc")
        convert_btc = ("BAT" in full_csv) or ("CRO" in full_csv)
        if convert_btc:
            BTCData = glob.glob("BTC.csv")
            debug_print("Convert_BTC è TRUE!")
            if not BTCData:
                raise FileNotFoundError(f"Per riprende {base} è necessario un csv di BTC completo!")
            if (btc_df is None or btc_df.empty) and not load_btc_csv_thread.is_alive():
                load_btc_csv_thread.start()
        df_full = pd.read_csv(full_csv, sep=";")
        df_full["timestamp"] = pd.to_datetime(
            df_full["timestamp"],
            format="%d/%m/%Y %H:%M",
            utc=True,
            errors="coerce"       #Stringhe non valide → NaT
        )
        if (DEBUGDF): debug_print("Dopo conversione df_full",df_full.head(3))
        df_full.dropna(subset=["timestamp"], inplace=True)
        if convert_btc:
            if load_btc_csv_thread.is_alive():
                debug_print("Attendo Thread BTC per riconversione EUR→BTC...")
                load_btc_csv_thread.join()
            def reverse_btc_conversion(row):
                timestamp = row["timestamp"]
                timestamp = timestamp.strftime('%d/%m/%Y %H:%M')
                try:
                    btc_price = get_btc_price(timestamp)
                    price = row["close"]
                    price = float(price.replace(",", "."))
                    if btc_price is None: 
                        debug_print(f"Prezzo BTC non trovato per timestamp {timestamp} completo.", level="e")
                        sys.exit(1)
                    else:
                        price /= btc_price
                        return price
                except Exception as e:
                    debug_print(f"Errore durante la riconversione EUR→BTC: {e}", level="w")
                    sys.exit(1)
                    
            df_full["close"] = df_full.apply(reverse_btc_conversion, axis=1)
            df_full["timestamp"] = (df_full["timestamp"].astype(int) // 10**6)
            df_full.dropna(subset=["timestamp"], inplace=True)
            df_full.to_csv(partial_csv, index=False, sep=";")
        else:    
            df_full["close"] = df_full["close"].str.replace(",", ".").astype(float)
            df_full["timestamp"] = (df_full["timestamp"].astype(int) // 10**6)
            df_full.dropna(subset=["timestamp"], inplace=True)
            df_full.to_csv(partial_csv, index=False, sep=";")
        
        if df_full.empty:
             raise ValueError(f"{partial_csv} non risulta essere riconvertito correttamente!")
        return int(df_full.iloc[-1]["timestamp"])
    except Exception as e:
        debug_print(e, level="e")
        print(f"{partial_csv} non risulta essere riconvertito correttamente!")
        sys.exit(1)
# ────────────────────────────────────────────────────────────────────────────────
# Download singolo simbolo
# ────────────────────────────────────────────────────────────────────────────────
def download_symbol(exchange: ccxt.Exchange, symbol: str) -> bool:
    global current_pair_idx, stop_flag, last_symbol, last_timestamp_str
    
    if timeframe_ms is None: raise ValueError(f"Timeframe non supportato: {TIMEFRAME}")

    current_pair_idx += 1
    last_symbol = symbol
    last_timestamp_str = None
    convert_btc = "/BTC" in symbol
    if convert_btc and (btc_df is None or btc_df.empty) and not load_btc_csv_thread.is_alive():
        load_btc_csv_thread.start()

    base = symbol.split("/")[0]
    full_csv    = f"{base}.csv"
    partial_csv = f"{base}_partial.csv"

    df_partial = load_csv_safe(partial_csv)
    df_full= load_csv_safe(full_csv)
    if (BREAKPOINT): input("Breakpoint")
    # — RIPARTENZA —
    if not df_partial.empty:
        ts_val = df_partial.iloc[-1]["timestamp"]
        if pd.api.types.is_numeric_dtype(df_partial["timestamp"]):
            since = int(ts_val) + timeframe_ms
            last_timestamp_str = datetime.fromtimestamp(int(ts_val) / 1000, tz=timezone.utc).strftime("%d/%m/%Y %H:%M")
        else:
            dt = datetime.strptime(str(ts_val), "%d/%m/%Y %H:%M")
            since = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000) + timeframe_ms
            last_timestamp_str = str(ts_val)
        print(f"\nRiprendo {symbol} con timeframe {TIMEFRAME} da timestamp {last_timestamp_str} (parziale)")

    elif not df_full.empty:
        if not skipanswer: 
            answer = input(f"Trovato file completo per {symbol}. Vuoi aggiornarlo? (s/n): ").strip().lower()
            if answer != "s":
                print(f"Salto aggiornamento {symbol}.")
                return True

        since = full_to_partial_conversion(full_csv, partial_csv) + timeframe_ms
        last_timestamp_str = df_full.iloc[-1]["timestamp"]
        print(f"\nAggiornamento {symbol} dal {last_timestamp_str} con timeframe {TIMEFRAME}")

    else:
        since = start_ts_ms
        print(f"\nNessun file precedente per {symbol}, parto con timeframe {TIMEFRAME} da {START_DATE}")

    buffer: list = []
    batch_counter = 0
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    retry_count = 0
    rate_limit_s = (getattr(exchange, "rateLimit", 1000) / 1000.0) * RATE_LIMIT_SAFETY

    while since < now_ms:
        check_escape()
        if stop_flag:
            break

        t0 = time.time()
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME,
                                         since=since, limit=BATCH_LIMIT)
            retry_count = 0
        except Exception as e:
            if buffer:
                save_partial(buffer, partial_csv)
                buffer.clear()
            retry_count += 1
            progress_print([
                f"ERRORE ({retry_count}/{MAX_RETRIES}) su {symbol}: {e}",
                "Ritentativo tra 5 s…",
            ])
            if retry_count >= MAX_RETRIES:
                print(f"\nInterrotto: impossibile proseguire per errore: {e}")
                return False
            time.sleep(5)
            continue

        if not ohlcv:
            break

        for ts, _, _, _, close, *_ in ohlcv:
            buffer.append([ts, close])
            last_timestamp_str = datetime.fromtimestamp(ts / 1000,
                                tz=timezone.utc).strftime("%d/%m/%Y %H:%M")

        last_ts = ohlcv[-1][0]
        since   = last_ts + timeframe_ms
        batch_counter += 1

        pair_progress    = (last_ts - start_ts_ms) / (now_ms - start_ts_ms)
        overall_progress = (current_pair_idx - 1 + pair_progress) / total_pairs
        progress_print([
            f"Operazione {current_pair_idx}/{total_pairs}:",
            f"Coppia attuale: {symbol}",
            f"Recuperate {len(ohlcv)} candele - {last_timestamp_str}",
            f"Completato: {overall_progress * 100:6.2f} %",
        ])

        if batch_counter % PARTIAL_EVERY == 0:
            save_partial(buffer, partial_csv)
            buffer.clear()

        elapsed = time.time() - t0
        time.sleep(max(0.0, rate_limit_s - elapsed))

    if stop_flag:
        if buffer:
            save_partial(buffer, partial_csv)
        return False

    if buffer:
        save_partial(buffer, partial_csv)

    consolidate_csv(full_csv, partial_csv, convert_btc)
    print()  # riga vuota
    return True

# ────────────────────────────────────────────────────────────────────────────────
# Formattazione di tutti i parziali nella cartella
# ────────────────────────────────────────────────────────────────────────────────
def format_all_partials() -> None:
    ConvertSkipped = False
    partial_files = glob.glob("*_partial.csv")
    if not partial_files:
        print("Nessun file parziale da formattare trovato.")
        return
    for partial_csv in partial_files:
        base = partial_csv[:-12]
        full_csv = f"{base}.csv"
        convert_btc = ("BAT" in base) or ("CRO" in base)
        if convert_btc:
                BTCData = glob.glob("BTC.csv")
                if not BTCData:
                    print(f"Per formattare {base} è necessario un csv di BTC completo!")
                    ConvertSkipped = True
                    continue  # salta questo file
                if (btc_df is None or btc_df.empty) and not load_btc_csv_thread.is_alive():
                    load_btc_csv_thread.start()
        consolidate_csv(full_csv, partial_csv, convert_btc)
    if ConvertSkipped: 
        print("Formattazione completata per i file parziali(Saltati CRO/BAT, csv BTC mancante.).")
    else:
        print("Formattazione completata per tutti i file parziali.")

# ────────────────────────────────────────────────────────────────────────────────
# Verifica gap dei dati
# ────────────────────────────────────────────────────────────────────────────────

def verify_data(csv_file: str) -> bool:
    global timeframe_ms

    print(f"Verifica dati: {csv_file}")
    df = pd.read_csv(csv_file, sep=";")
    if "timestamp" not in df.columns or "close" not in df.columns:
        print("Il file non contiene colonne 'timestamp' o 'close' valide.")
        return False

    # Controllo formato timestamp
    def check_ts_format(ts):
        try:
            datetime.strptime(str(ts), "%d/%m/%Y %H:%M")
            return True
        except Exception:
            return False

    if not df["timestamp"].apply(check_ts_format).all():
        print("Alcuni timestamp non sono nel formato corretto.")
        return False

    timestamps = pd.to_datetime(df["timestamp"], format="%d/%m/%Y %H:%M")
    diffs = timestamps.diff().dropna()

    expected = timeframe_ms / 60000  # type: ignore # minuti (es 1, 5, 15...)

    gaps = diffs[diffs != pd.Timedelta(minutes=expected)]

    if not gaps.empty:
        total_gaps = len(gaps)
        print(f"Trovati {total_gaps} gap temporali non conformi a {int(expected)}m.")
        print("Gap trovati:")

        to_print = gaps.index if total_gaps <= 10 else gaps.index[:10]

        for idx in to_print:
            gap_min = int(gaps[idx].total_seconds() / 60)
            prev_time = timestamps[idx - 1].strftime("%d/%m/%Y %H:%M")
            curr_time = timestamps[idx].strftime("%d/%m/%Y %H:%M")
            print(f"C'è un gap temporale di {gap_min} minuti tra la data {prev_time} e la data {curr_time}")
            if DEBUG: debug_print(f"C'è un gap temporale di {gap_min} minuti tra la data {prev_time} e la data {curr_time}")

        if total_gaps > 10:
            print(f"E altri {total_gaps - 10} gaps")
            if DEBUG:
                for idx in gaps.index:
                    gap_min = int(gaps[idx].total_seconds() / 60)
                    prev_time = timestamps[idx - 1].strftime("%d/%m/%Y %H:%M")
                    curr_time = timestamps[idx].strftime("%d/%m/%Y %H:%M")
                    debug_print(f"C'è un gap temporale di {gap_min} minuti tra la data {prev_time} e la data {curr_time}")

        return False

    print("Nessun problema trovato nei timestamp.")
    return True

# ────────────────────────────────────────────────────────────────────────────────
# Funzione di restore.
# ────────────────────────────────────────────────────────────────────────────────
def restore_from_gaps(csv_file: str, exchange: ccxt.Exchange) -> None:
    print(f"Ripristino dati per: {csv_file}")
    df = pd.read_csv(csv_file, sep=";")
    
    # Identifica gap come in verify_data ma salvando le date con gap
    timestamps = pd.to_datetime(df["timestamp"], format="%d/%m/%Y %H:%M")
    diffs = timestamps.diff().dropna()
    expected = timeframe_ms / 60000  # type: ignore # minuti
    
    gap_indices = diffs[diffs != pd.Timedelta(minutes=expected)].index
    if gap_indices.empty:
        print("Nessun gap da riparare.")
        return
    
    # Date da rimuovere (tutta la giornata della data dei gap)
    dates_to_remove = set()
    sorted_indices = sorted(gap_indices)
    for idx in sorted_indices:
        dt_prev = timestamps[idx - 1].date()
        dt_curr = timestamps[idx].date()
        # Aggiungi tutte le date dall'inizio alla fine del gap
        start = min(dt_prev, dt_curr)
        end = max(dt_prev, dt_curr)
        current = start
        while current <= end:
            dates_to_remove.add(current)
            current += timedelta(days=1)
    
    print(f"Date con gap da riparare: {', '.join(str(d) for d in sorted(dates_to_remove))}")
    
    # Converte csv completo in parziale
    partial_csv = csv_file.replace(".csv", "_partial.csv")
    full_to_partial_conversion(csv_file, partial_csv)
    
    # Carica parziale
    df_partial = pd.read_csv(partial_csv, sep=";")
    
    # Rimuovi righe del parziale per le date da riparare
    df_partial["date"] = pd.to_datetime(df_partial["timestamp"], unit='ms', utc=True).dt.date
    df_partial = df_partial[~df_partial["date"].isin(dates_to_remove)]
    df_partial.drop(columns=["date"], inplace=True)
    
    # Debug stato rimozione date
    if DEBUG: debug_print("Rimozione date con gap dal parziale", partial_csv, dates=sorted(dates_to_remove), rows_count=len(df_partial))
    
    # Riscrivi parziale pulito
    df_partial.to_csv(partial_csv, index=False, sep=";")
    
    # Per ogni data da riparare, scarica dati da exchange e aggiungi a parziale
    total_dates = len(dates_to_remove)
    for i, dt in enumerate(sorted(dates_to_remove), 1):
        start_dt = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
        since = int(start_dt.timestamp() * 1000)
        until = since + 86400 * 1000  # +1 giorno in ms
        
        progress_print([
            f"Ripristino {csv_file} ({i}/{total_dates}):",
            f"Scarico dati per {dt}...",
        ])
        
        all_ohlcv = []
        fetch_since = since
        while fetch_since < until:
            try:
                symbol = f"{csv_file[:-4]}/{'BTC' if csv_file[:-4] in ['CRO', 'BAT'] else 'EUR'}"
                ohlcv = exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=TIMEFRAME,
                    since=fetch_since,
                    limit=BATCH_LIMIT,
                )
            except Exception as e:
                print(f"Errore fetch: {e}")
                break
            if not ohlcv:
                break
            all_ohlcv.extend(ohlcv)
            last_ts = ohlcv[-1][0]
            fetch_since = last_ts + timeframe_ms
            if last_ts >= until:
                break
        
        # Buffer dati da aggiungere
        buffer = []
        for ts, _, _, _, close, *_ in all_ohlcv:
            if since <= ts < until:
                buffer.append([ts, close])
        
        if buffer:
            save_partial(buffer, partial_csv)
            if DEBUG: debug_print(f"Download completato per data {dt}, {partial_csv}")
            if DEBUGDF: debug_print(f"{buffer}")
    
    # Riconverti in csv completo
    convert_btc = "/BTC" in symbol # type: ignore
    RestoreSkipped = False
    if convert_btc:
        BTCData = glob.glob("BTC.csv")
        if not BTCData:
            print(f"Per ripristinare {base} è necessario un csv di BTC completo!")
            RestoreSkipped = True
        else:
            if (btc_df is None or btc_df.empty) and not load_btc_csv_thread.is_alive():
                load_btc_csv_thread.start()
            consolidate_csv(csv_file, partial_csv, convert_btc)
    else: consolidate_csv(csv_file, partial_csv, convert_btc)
    if DEBUG: (f"Consolidamento completato {csv_file}")
    if RestoreSkipped:
        print(f"Ripristino fallito per {csv_file}(BTC.csv completo mancante!)\n")
    else:
        print(f"Ripristino completato per {csv_file}")

        
# ────────────────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Downloader OHLCV da Binance e Cryptocom"
    )
    parser.add_argument(
        "-format", action="store_true",
        help="Formatta i file parziali in file completi"
    )
    parser.add_argument(
        "-verify", action="store_true",
        help="Verifica integrità dati dei file CSV completi"
    )
    parser.add_argument(
        "-restore", action="store_true",
        help="Ripara gap nei file CSV completi riscaricando i dati mancanti"
    )
    parser.add_argument(
        "-auto", action="store_true",
        help="Modalità download automatica(Skip input utente)"
    )
    args = parser.parse_args()

    if args.format:
        format_all_partials()
        sys.exit(0)
    
    if args.verify:
                # verifica file CSV completi
        csv_files = [f for f in glob.glob("*.csv") if "_partial" not in f]
        if not csv_files:
            print("Nessun file CSV completo trovato per la verifica.")
            sys.exit(0)

        valid_bases = {sym.split("/")[0] for syms in exchanges.values() for sym in syms}
        csv_files = [f for f in csv_files if f[:-4] in valid_bases]

        if not csv_files:
            print("Nessun file CSV valido da verificare.")
            sys.exit(0)

        all_ok = True
        for csv_file in csv_files:
            if not verify_data(csv_file):
                all_ok = False
        if all_ok:
            print("\nTutti i file CSV sono consistenti.")
        else:
            print("\nAlcuni file CSV presentano anomalie. Usa -restore per risolvere le anomalie.")
        sys.exit(0)
        
    if args.restore:
        csv_files = [f for f in glob.glob("*.csv") if "_partial" not in f]
        valid_bases = {sym.split("/")[0] for syms in exchanges.values() for sym in syms}
        csv_files = [f for f in csv_files if f[:-4] in valid_bases]

        if not csv_files:
            print("Nessun file CSV valido da riparare.")
            sys.exit(0)

        base_to_exchange = {}
        for exch, syms in exchanges.items():
            for sym in syms:
                base = sym.split("/")[0]
                base_to_exchange[base] = exch

        for csv_file in csv_files:
            base = csv_file[:-4]
            exch = base_to_exchange.get(base)
            if not exch:
                print(f"Exchange non trovato per {csv_file}, skip.")
                continue
            restore_from_gaps(csv_file, exch)

        print("Ripristino completato per tutti i file.")
        sys.exit(0)
    
    if args.auto:
        print("Premi Ctrl+E per interrompere l'operazione.\n")
        skipanswer = True
        try:
            for exch, symbols in exchanges.items():
                for sym in symbols:
                    result = download_symbol(exch, sym)
                    if not result:
                        print("\nDownload interrotto dall'utente.")
                        sys.exit(0)
            print("\nDownload completato!")
        except KeyboardInterrupt:
            print("\nEsecuzione interrotta dall'utente.")
            sys.exit(0)
    
    #Senza argomenti parte il downloader.
    if not (args.auto) and not (args.restore) and not (args.verify) and not (args.format):
        print("Premi Ctrl+E per interrompere l'operazione.\n")
        try:
            for exch, symbols in exchanges.items():
                for sym in symbols:
                    result = download_symbol(exch, sym)
                    if not result:
                        print("\nDownload interrotto dall'utente.")
                        sys.exit(0)
            print("\nDownload completato!")
        except KeyboardInterrupt:
            print("\nEsecuzione interrotta dall'utente.")
            sys.exit(0)