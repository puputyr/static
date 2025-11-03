import requests
import pandas as pd
from bs4 import BeautifulSoup
import html
from io import StringIO
from sqlalchemy import create_engine, text
import re
from scripts.db_config_statictable import get_db_engine
def run_etl_kode_1():
    """Fungsi utama untuk ETL 'kode 1' (Tabel 1274)"""
    print("Menjalankan ETL untuk Tabel 1274 (Proyeksi Penduduk menurut provinsi)...")
    # mengambil data dari API
url1 = "https://webapi.bps.go.id/v1/api/view/domain/0000/model/statictable/lang/ind/id/1274/key/79452e4c302f8921ad36cd2bf55f0630/"
response3 = requests.get(url1)
data3 = response3.json()

# decode & parse HTML
raw3 = data3['data']['table']
html3 = html.unescape(raw3)
soup3 = BeautifulSoup(html3, 'html.parser')
tables3 = pd.read_html(StringIO(str(soup3)), header=None)
df3 = tables3[0]

# identifikasi header & data
header_idx = 3
start, end = 4, 43
years3 = [str(int(float(df3.iloc[header_idx, col]))) for col in range(1, df3.shape[1])]
df3 = df3.iloc[start:end + 1].copy()
df3.columns = ['provinsi'] + years3

# cleaning & reshape
for col in years3:
    df3[col] = pd.to_numeric(df3[col], errors='coerce')

df3_melted = df3.melt(
    id_vars=['provinsi'],
    var_name='tahun',
    value_name='proyeksi_penduduk'
)

df3_melted['tahun'] = pd.to_numeric(df3_melted['tahun'], errors='coerce').astype('Int64')
df3_melted.dropna(subset=['proyeksi_penduduk'], inplace=True)
df3_melted['proyeksi_penduduk'] = df3_melted['proyeksi_penduduk'].map(lambda x: f"{x:.2f}")
df3_melted.reset_index(drop=True, inplace=True)

# menambahkan kategori
subsca_id3 = data3['data']['subcsa_id']
df3_melted.insert(0, 'id_kategori', subsca_id3)
df3_melted.head()


# mengambil raw dari API
subcsa = data3['data']['subcsa'].lower()
title = data3['data']['title'].lower()

# schema
subcsa_clean = re.sub(r'\bdan\b', '', subcsa)
schema = 'api_bps_' + re.sub(r'\W+', '_', subcsa_clean.strip())

# table
name = title.replace(',', '_').strip()
noparens = re.sub(r'\([^)]*\)', '', name)
nonum = re.sub(r'\d+', '', noparens)
novowel = re.sub(r'[aiueo]', '', nonum)
nopunct = re.sub(r'[^\w\s]', '', novowel)
table = re.sub(r'\s+', '_', nopunct.strip())

# merapikan underscore
table = re.sub(r'_+', '_', table).strip('_')

print(schema)
print(table)



# koneksi database
# engine = create_engine(
#     "postgresql+psycopg2://postgres:2lNyRKW3oc9kan8n@103.183.92.158:5432/result_cleansing"
# )
engine = get_db_engine()
# cek schema
with engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    conn.execute(text(f"""
        DROP TABLE IF EXISTS {schema}."{table}";
        CREATE TABLE {schema}."{table}" (
            id SERIAL PRIMARY KEY,
            id_kategori INT,
            "provinsi" TEXT,
            "proyeksi_penduduk" FLOAT,
            "tahun" INT
        );
    """))

# masukkan data ke tabel (tanpa kolom id)
df3_melted.to_sql(table, engine, schema=schema, if_exists='append', index=False)
print(f"Sukses memuat data ke {schema}.\"{table}\"")
