from collections import defaultdict

import pandas as pd


def print_dataframe_col_per_alphanumeric(df: pd.DataFrame, column: str):
    groups = defaultdict(list)
    for entry in df[column].unique():
        first_char = entry[0].upper()  # Convert to uppercase for consistent grouping
        groups[first_char].append(entry)

    # Print the grouped entries
    for first_char, entries in sorted(groups.items()):
        print(f"{first_char} ({len(entries)}): {', '.join(entries)}")
