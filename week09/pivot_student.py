from pathlib import Path
import sqlite3
import pandas as pd
ROOT=Path(__file__).resolve().parent
with sqlite3.connect((ROOT/'data'/'warehouse.db').as_uri()+'?mode=ro',uri=True) as con:
    df=pd.read_sql_query('SELECT * FROM sales',con)
print('=== Sales Head ===')
print(df.head())

# P1: province x month, sum(amount), fill_value=0, margins=True, margins_name='Total'
p1 = df.pivot_table(
    index='province',
    columns='month',
    values='amount',
    aggfunc='sum',
    fill_value=0,
    margins=True,
    margins_name='Total'
)
print('\n=== P1: Province x Month ===')
print(p1)

# P2: filter September, then category x province
sep_df = df[df['month'] == '2026-09']
p2 = sep_df.pivot_table(
    index='category',
    columns='province',
    values='amount',
    aggfunc='sum',
    fill_value=0,
    margins=True,
    margins_name='Total'
)
print('\n=== P2: September Category x Province ===')
print(p2)

# P3: assert that the pivot grand total equals df['amount'].sum()
grand_total = p1.loc['Total', 'Total']
actual_sum = df['amount'].sum()
assert grand_total == actual_sum, f"Grand total mismatch: {grand_total} != {actual_sum}"
print(f'\n=== P3: Assert passed! Grand Total = {grand_total} ===')

# P4: export each result to CSV in your submission folder
p1_path = ROOT / 'pivot_province_month.csv'
p2_path = ROOT / 'pivot_september.csv'
p1.to_csv(p1_path)
p2.to_csv(p2_path)
print(f'\n=== P4: Exported to {p1_path.name} and {p2_path.name} ===')
