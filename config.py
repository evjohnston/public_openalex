import os

base_folder = "DeepSeek"

input_folder = os.path.join(base_folder, "JSONs")
output_folder = os.path.join(base_folder, "CSVs")

os.makedirs(output_folder, exist_ok=True)