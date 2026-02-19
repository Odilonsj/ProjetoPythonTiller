"""
GOLD LAYER - SLA Analysis and Assignee Performance
Analysis-ready aggregations with SLA compliance metrics
"""
import pandas as pd

# Read Parquet file
df = pd.read_parquet("silver_issues.parquet")

# Add a filter to show every status but open
df = df[df["status"] != "Open"]

print("\n=== First 5 Rows ===")
print(df.head())

print(f"\n=== Shape ===")
print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")

print("\n=== Full Dataset - Resolution Time and SLA Analysis ===")
display_cols = ["id", "issue_type","assignee_id", "assignee_name","priority", "created_at","resolved_at", "resolution_hours", "sla_expected_hours", "sla_met"]
print(df.sort_values(["assignee_id", "resolved_at"])[display_cols].to_string(index=False))

# ===== Save Gold Layer to Parquet =====
df_sorted = df.sort_values(["assignee_id", "resolved_at"]).reset_index(drop=True)
df_sorted.to_parquet("gold_issues_full.parquet", index=False, engine="pyarrow")

# Aggregation 1: SLA Compliance by Priority
print("\n=== SLA Compliance by Priority ===")
priority_summary = df.groupby("priority").agg(
    total_issues=("id", "count"),
    met_sla=("sla_met", "sum"),
    failed_sla=("sla_met", lambda x: (~x).sum()),
    compliance_pct=("sla_met", lambda x: round(x.sum() / len(x) * 100, 2))
).reset_index()
print(priority_summary.to_string(index=False))

# Aggregation 2: Per-Assignee Performance (including SLA metrics)
print("\n=== Per-Assignee Performance with SLA Metrics ===")
assignee_summary = (
    df.groupby("assignee_id", dropna=False)
      .agg(
          issues_count=("id", "count"),
          total_resolution_hours=("resolution_hours", "sum"),
          avg_resolution_hours=("resolution_hours", "mean"),
          median_resolution_hours=("resolution_hours", "median"),
          sla_met_count=("sla_met", "sum"),
          sla_compliance_pct=("sla_met", lambda x: round(x.sum() / len(x) * 100, 2))
      )
      .reset_index()
)

assignee_summary["total_resolution_hours"] = assignee_summary["total_resolution_hours"].round(2)
assignee_summary["avg_resolution_hours"] = assignee_summary["avg_resolution_hours"].round(2)
assignee_summary["median_resolution_hours"] = assignee_summary["median_resolution_hours"].round(2)

print(assignee_summary.sort_values("total_resolution_hours", ascending=False).to_string(index=False))







