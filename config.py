import os

# 🔹 Change this one line to set the base folder
base_folder = "DeepMind"

input_folder = os.path.join(base_folder, "JSONs")
output_folder = os.path.join(base_folder, "CSVs")

os.makedirs(output_folder, exist_ok=True)

# --- Run all steps when executed directly: python config.py ---
if __name__ == "__main__":
    import subprocess
    import sys
    import time

    steps = [
        "step1_openalex_paper_json_scraper.py",
        "step2_combinecsvs.py",
        "step3_openalex_profile_scraper.py",
        "step4_openalex_profile_works_scraper.py",
        "step5_openalex_institutions_scraper.py",
        "step6_openalex_institutions_enriching.py",
        "step7_openalex_institutions_lineage.py",
        "step8_openalex_author_affiliations.py",
        "step9_combine_afil_back_into_enriched.py",
        "step10_careertrajectory.py",
        "step11_visualizations.py",
    ]

    for i, script in enumerate(steps, 1):
        print(f"\n{'='*60}")
        print(f"Running step {i}/11: {script}")
        print(f"{'='*60}\n")

        start = time.time()
        result = subprocess.run([sys.executable, script])
        elapsed = time.time() - start

        if result.returncode != 0:
            print(f"\n❌ {script} failed (exit code {result.returncode}). Stopping.")
            sys.exit(1)

        print(f"\n✅ {script} finished in {elapsed:.1f}s")

    print(f"\n{'='*60}")
    print("All 11 steps completed.")
    print(f"{'='*60}")