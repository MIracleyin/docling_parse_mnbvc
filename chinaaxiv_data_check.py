from chinaxiv_to_mm import ChinaXivBlock
from pathlib import Path
import pandas as pd

def main():
    parquet_file = Path("outputs/demo_0.parquet")
    df = pd.read_parquet(parquet_file)
    # df to dict
    rows = df.to_dict(orient="records")
    # df to blocks
    for row in rows:
        block = ChinaXivBlock()
        block.from_dict(row)
        print(block)
    
if __name__ == "__main__":
    main()
    