# dags/bps_scripts/db.py

from sqlalchemy import create_engine

# Definisikan koneksi Anda di sini
DB_STRING = "postgresql+psycopg2://postgres:2lNyRKW3oc9kan8n@103.183.92.158:5432/result_cleansing"

def get_db_engine():
    """Mengembalikan engine SQLAlchemy."""
    return create_engine(DB_STRING)