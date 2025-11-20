
import pandas as pd
from pathlib import Path

input_dir = Path(".")
output_file = Path("fred_all.tsv")


count = 0

with output_file.open("w", encoding="utf-8") as out_f:
    out_f.write("series_id\tdate\tvalue\n") 
    for csv_file in sorted(input_dir.glob("fred_*.csv")):
        try:
            df = pd.read_csv(csv_file)
            series_id = csv_file.stem.replace("fred_", "")
            for _, row in df.iterrows():
                out_f.write(f"{series_id}\t{row['date']}\t{row['value']}\n")
            csv_file.unlink()  
            count += 1
        except Exception as e:
            print(f"Error in {csv_file.name}: {e}")

print(f" Done. Merged {count} series into {output_file}")
