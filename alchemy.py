#pip install pandas sqlalchemy    

import sqlalchemy
import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, text

# Wczytanie danych z plików CSV
stations_df = pd.read_csv('clean_stations.csv')
measures_df = pd.read_csv('clean_measure.csv')

# Utworzenie silnika bazy danych SQLite za pomocą SQLAlchemy
engine = create_engine('sqlite:///database.db')


# Utworzenie obiektu MetaData
metadata = MetaData()

# Definiowanie tabeli 'stations'
stations = Table(
    'stations', metadata,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)

# Definiowanie tabeli 'measures'
measures = Table(
    'measures', metadata,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Float)
)

# Tworzenie tabel w bazie danych
metadata.create_all(engine)

# Zapisanie danych do tabel w bazie danych
stations_df.to_sql('stations', con=engine, if_exists='replace', index=False)
measures_df.to_sql('measures', con=engine, if_exists='replace', index=False)

# Wykonanie przykładowego zapytania + context manager
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM stations LIMIT 5")).fetchall()
    print(result)
