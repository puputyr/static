import requests
import pandas as pd
from bs4 import BeautifulSoup
import html
from io import StringIO
from sqlalchemy import create_engine, text
import re
from scripts.db_config_statictable import get_db_engine
def run_etl_kode_2():
    """Fungsi utama untuk ETL 'kode 2' (Tabel 1613)"""
    print("Menjalankan ETL untuk Tabel 1613 (APS seIndonesia menurut Klasifikasi pedesaan perkotaan)...")
    # mengambil data dari API
# mengambil data dari API
url2 = "https://webapi.bps.go.id/v1/api/view/domain/0000/model/statictable/lang/ind/id/1613/key/79452e4c302f8921ad36cd2bf55f0630/"
response1 = requests.get(url2)
data1 = response1.json()

# decode & parse HTML
html_raw1 = data1['data']['table']
html_clean1 = html.unescape(html_raw1)
soup1 = BeautifulSoup(html_clean1, 'html.parser')
tables1 = pd.read_html(StringIO(str(soup1)))
df1 = tables1[0]

# cleaning data
df1_cleaned = df1.drop([0, 1, 2, 3, 4, 15]).copy()

new_columns1 = []
for col in df1_cleaned.columns:
    if col in [0, 1]:
        new_columns1.append(df1_cleaned.iloc[0, col])
    else:
        kelompok_umur = df1.iloc[3, col]
        jenis_aps = df1.iloc[4, col]
        tahun = df1_cleaned.iloc[0, col]
        new_columns1.append(f"{kelompok_umur} - {jenis_aps} - {tahun}")

df1_cleaned.columns = new_columns1
df1_cleaned = df1_cleaned.drop(5).reset_index(drop=True)

df1_cleaned = df1_cleaned.rename(columns={
    df1_cleaned.columns[0]: 'klasifikasi_desa',
    df1_cleaned.columns[1]: 'jenis_kelamin'
})

# reshape data
df1_melted = df1_cleaned.melt(
    id_vars=['klasifikasi_desa', 'jenis_kelamin'],
    var_name='kelompok_umur - jenis_aps - tahun',
    value_name='value'
)

df1_melted[['kelompok_umur', 'jenis_aps', 'tahun']] = df1_melted[
    'kelompok_umur - jenis_aps - tahun'
].str.split(' - ', expand=True)

df1_melted = df1_melted.drop(columns=['kelompok_umur - jenis_aps - tahun'])
df1_melted = df1_melted[['klasifikasi_desa', 'jenis_kelamin', 'kelompok_umur', 'value', 'jenis_aps', 'tahun']]

# menambahkan id kategori
subsca_id1 = data1['data']['subcsa_id']
df1_melted.insert(0, 'id_kategori', subsca_id1)
df1_melted.head(290)


# mengambil raw dari API
subcsa = data1['data']['subcsa'].lower()
title = data1['data']['title'].lower()

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

# pastikan schema ada
# with engine.connect() as conn:
#     conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
engine = get_db_engine()

# buat tabel manual dengan kolom id di awal + tipe kolom sesuai instruksi
with engine.begin() as conn:
    conn.execute(text(f"""
        DROP TABLE IF EXISTS {schema}."{table}";
        CREATE TABLE {schema}."{table}" (
            id SERIAL PRIMARY KEY,
            id_kategori INT,
            "klasifikasi_desa" TEXT,
            "jenis_kelamin" TEXT,
            "kelompok_umur" TEXT,
            "value" FLOAT,
            "jenis_aps" TEXT,
            "tahun" INT
        );
    """))

# masukkan data ke tabel (tanpa kolom id)
df1_melted.to_sql(table, engine, schema=schema, if_exists='append', index=False)
print(f"Sukses memuat data ke {schema}.\"{table}\"")