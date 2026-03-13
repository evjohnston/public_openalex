import re
import pandas as pd
from pathlib import Path

# Path to your CSV
file_path = Path("KimiK2/CSVs/paper1_metadata.csv")  # adjust if needed

# Your target names in "First Last" form
target_names = [
    "Angang Du", "Chenzhuang Du", "Dehao Zhang", "Enming Yuan", "Enzhe Lu",
    "Flood Sung", "Guokun Lai", "Hao Ding", "Hao Hu", "Hao Zhang", "Haotian Yao",
    "Hongcheng Gao", "Huabin Zheng", "Jianlin Su", "Jianzhou Wang", "Jingyuan Liu",
    "Junjie Yan", "Kimi Team", "Longhui Yu", "Mengnan Dong", "Shaowei Liu",
    "Weiran He", "Weixiao Huang", "Weixin Xu", "Xingzhe Wu", "Xinxing Zu",
    "Xinyu Zhou", "Yangyang Hu", "Yanru Chen", "Yejie Wang", "Yibo Liu",
    "Yiping Bao", "Yulun Du", "Yuxin Wu", "Yuzhi Wang", "Zaida Zhou", "Zhaowei Li",
    "Zheng Zhang", "Zhilin Yang", "Zhiqi Huang", "Zihao Huang", "Zongyu Lin"
]

def normalize(s: str) -> str:
    """Lowercase, remove periods, collapse whitespace."""
    s = s.strip().lower()
    s = s.replace(".", "")
    s = re.sub(r"\s+", " ", s)
    return s

def first_last_to_last_first(name: str) -> list[str]:
    """
    Turn 'First [Middle] Last' into ['Last, First', 'Last, First Middle'] (normalized).
    If name is not in that pattern (e.g., 'Kimi Team'), keep raw normalized as-is.
    """
    parts = name.strip().split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
        middle = " ".join(parts[1:-1]) if len(parts) > 2 else ""
        forms = [f"{last}, {first}"]
        if middle:
            forms.append(f"{last}, {first} {middle}")
        return [normalize(f) for f in forms]
    else:
        # Single token like 'Kimi' or special team name
        return [normalize(name)]

# Build a set of acceptable normalized targets in Raw format
target_raw_forms = set()
for n in target_names:
    for form in first_last_to_last_first(n):
        target_raw_forms.add(form)
    # also allow exact 'First Last' in case Raw field occasionally stores that
    target_raw_forms.add(normalize(n))

# Load CSV
df = pd.read_csv(file_path)

# Column that contains 'Last, First' author names
raw_col = "Raw Author Name"  # change if your column header differs exactly

def split_authors(cell: str) -> list[str]:
    """
    Split a cell that may contain one or multiple authors.
    Do NOT split on commas (commas separate last/first).
    """
    if pd.isna(cell):
        return []
    s = str(cell)
    # split on semicolon or vertical bar; add others if your data uses them
    if ";" in s or "|" in s:
        parts = re.split(r"[;|]", s)
    else:
        parts = [s]
    return [p.strip() for p in parts if p.strip()]

def row_matches(raw_cell: str) -> bool:
    # A row matches if ANY author in the cell matches one of our target forms
    for author in split_authors(raw_cell):
        if normalize(author) in target_raw_forms:
            return True
    return False

# Apply filtering
if raw_col not in df.columns:
    raise KeyError(f"Column '{raw_col}' not found. Columns are: {list(df.columns)}")

filtered_df = df[df[raw_col].apply(row_matches)]

# Overwrite the original file
filtered_df.to_csv(file_path, index=False)
print(f"Filtered {len(filtered_df)} rows written back to {file_path}")
