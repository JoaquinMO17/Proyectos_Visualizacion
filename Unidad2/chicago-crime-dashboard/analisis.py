import pandas as pd

df = pd.read_csv('./data/Crimes_-_2001_to_Present_20251023.csv')
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
df = df.dropna(subset=['Date'])

print(f"Fecha mínima: {df['Date'].min()}")
print(f"Fecha máxima: {df['Date'].max()}")
print(f"Total registros: {len(df):,}")