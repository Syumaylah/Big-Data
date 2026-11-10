import sys, os
import pandas as pd

IN_FILE  = sys.argv[1]
OUT_FILE = sys.argv[2]
BATAS_AWAL = None

KOLOM = ["STATION","Station_name","DATE","LATITUDE","LONGITUDE","temperature",
         "dew_point_temperature","sea_level_pressure","wind_speed","precipitation",
         "relative_humidity","visibility","COUNTRY_CODE","temperature_Quality_Code"]
NUM = ["LATITUDE","LONGITUDE","temperature","dew_point_temperature","sea_level_pressure",
       "wind_speed","precipitation","relative_humidity","visibility","temperature_Quality_Code"]

RANGE = {"temperature":(-10,50), "dew_point_temperature":(-10,40), "relative_humidity":(0,100),
         "wind_speed":(0,60), "sea_level_pressure":(900,1100), "precipitation":(0,300)}

NEGARA = {"TH":"Thailand","ID":"Indonesia","MY":"Malaysia","RP":"Filipina","BM":"Myanmar",
          "CB":"Kamboja","LA":"Laos","SN":"Singapura","BX":"Brunei","VM":"Vietnam"}

chunks = []
total = 0
for ch in pd.read_csv(IN_FILE, usecols=KOLOM, chunksize=200_000, low_memory=False):
    total += len(ch)
    for c in NUM:
        ch[c] = pd.to_numeric(ch[c], errors="coerce")
    chunks.append(ch)
df = pd.concat(chunks, ignore_index=True)
print("1. Baris awal            :", total)

# - Tanggal -
df["DATE"] = pd.to_datetime(df["DATE"], format="%Y-%m-%dT%H:%M:%S", errors="coerce")
df = df.dropna(subset=["DATE","temperature","STATION"])
print("2. Setelah buang null    :", len(df))

# - Batas data -
if BATAS_AWAL:
    df = df[df["DATE"] >= BATAS_AWAL]
    print("   Setelah batas tanggal  :", len(df))

# - Kode kualitas buruk -
df = df[~df["temperature_Quality_Code"].isin([2,3,6,7])]
print("3. Setelah filter kualitas:", len(df))

# - Duplikat -
df = df.drop_duplicates(subset=["STATION","DATE"])
print("4. Setelah buang duplikat:", len(df))

# - Outlier: suhu di luar rentang -
lo, hi = RANGE["temperature"]
df = df[df["temperature"].between(lo, hi)].copy()
for c, (lo, hi) in RANGE.items():
    if c == "temperature": continue
    df.loc[~df[c].between(lo, hi), c] = float("nan")
print("5. Setelah buang outlier :", len(df))

# - Nama negara & jam -
df["negara"] = df["COUNTRY_CODE"].map(NEGARA).fillna(df["COUNTRY_CODE"])
df["jam"] = df["DATE"].dt.hour

# - Normalisasi Min-Max (0-1) -
for c, (lo, hi) in RANGE.items():
    df[c + "_norm"] = ((df[c] - lo) / (hi - lo)).round(5)

df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d %H:%M:%S")
df = df.drop(columns=["temperature_Quality_Code"])
os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

tmp = OUT_FILE + ".tmp"
df.to_csv(tmp, index=False)
os.replace(tmp, OUT_FILE)
print("6. Baris akhir disimpan  :", len(df), "->", OUT_FILE)
print("Kolom:", list(df.columns))
print("Tidak kosong (%):"); print((100*(1-df.isna().mean())).round(1).to_string())