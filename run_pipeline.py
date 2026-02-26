from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parent


def run_stage(label: str, module_name: str) -> None:
    print(f"[{label}] Running module {module_name}")
    subprocess.run(
        [sys.executable, "-m", module_name],
        check=True,
        cwd=PROJECT_ROOT,
    )


def main() -> None:
    print("Running full data pipeline (Bronze -> Silver -> Gold -> Reports)...")
    print(f"Using Python: {sys.executable}")

    run_stage("1/5", "src.bronze.ingest_bronze")
    run_stage("2/5", "src.bronze.fetch_holidays")
    run_stage("3/5", "src.silver.transform_silver")
    run_stage("4/5", "src.gold.build_gold")
    run_stage("5/5", "src.gold.build_sla_reports")

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()