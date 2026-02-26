"""
GOLD REPORTS - Average SLA Outputs
Generates CSV reports with average SLA in hours for:
1) Analyst
2) Issue type
"""

from pathlib import Path

import pandas as pd


project_root = Path(__file__).resolve().parents[2]
gold_input_file = project_root / "data" / "gold" / "gold_issues_full.parquet"
avg_sla_by_analyst_file = project_root / "data" / "gold" / "avg_sla_by_analyst.csv"
avg_sla_by_issue_type_file = project_root / "data" / "gold" / "avg_sla_by_issue_type.csv"

# This function generates CSV reports with average SLA in hours for analysts and issue types
def main() -> None:
    df = pd.read_parquet(gold_input_file)
    df = df[df["status"] != "Open"]

    avg_sla_by_analyst = (
        df.assign(
            assignee_id=df["assignee_id"].fillna("Unassigned"),
            assignee_name=df["assignee_name"].fillna("Unassigned")
        )
        .groupby(["assignee_id", "assignee_name"], dropna=False)
        .agg(
            issues_quantity=("id", "count"),
            avg_sla_hours=("resolution_hours", "mean"),
        )
        .reset_index()
        .sort_values(["avg_sla_hours", "issues_quantity"], ascending=[False, False])
    )
    avg_sla_by_analyst["avg_sla_hours"] = avg_sla_by_analyst["avg_sla_hours"].round(2)

    avg_sla_by_issue_type = (
        df.groupby("issue_type", dropna=False)
        .agg(
            issues_quantity=("id", "count"),
            avg_sla_hours=("resolution_hours", "mean"),
        )
        .reset_index()
        .sort_values(["avg_sla_hours", "issues_quantity"], ascending=[False, False])
    )
    avg_sla_by_issue_type["avg_sla_hours"] = avg_sla_by_issue_type["avg_sla_hours"].round(2)

    avg_sla_by_analyst_file.parent.mkdir(parents=True, exist_ok=True)
    avg_sla_by_analyst.to_csv(avg_sla_by_analyst_file, index=False, encoding="utf-8")
    avg_sla_by_issue_type.to_csv(avg_sla_by_issue_type_file, index=False, encoding="utf-8")

    print("CSV reports generated successfully:")
    print(f"- {avg_sla_by_analyst_file}")
    print(f"- {avg_sla_by_issue_type_file}")


if __name__ == "__main__":
    main()