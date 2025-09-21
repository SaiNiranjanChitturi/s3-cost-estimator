# MIT License
#
# Copyright (c) 2025
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import boto3

class S3StorageCostEstimator:
    """
    A Python library to estimate AWS S3 storage costs for a specific bucket using AWS Cost Explorer.
    Focuses only on storage-related costs (e.g., TimedStorage and EarlyDelete fees).
    Generates a text file with a table of costs for specified periods (daily, weekly, quarterly, annual).
    """

    PERIODS = ("day", "week", "quarter", "year")

    def __init__(
        self,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
        )
        self._region_name = region_name
        self.ce = self.session.client("ce")

    @staticmethod
    def _date_range(period: str) -> tuple[str, str]:
        now = datetime.utcnow()
        if period == "day":
            start = now - timedelta(days=1)
        elif period == "week":
            start = now - timedelta(weeks=1)
        elif period == "quarter":
            start = now - timedelta(days=90)
        elif period == "year":
            start = now - timedelta(days=365)
        else:
            raise ValueError("period must be one of: day, week, quarter, year")
        return start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")

    def _get_service_filter(self) -> Dict:
        """Filter for S3 service only."""
        return {
            "Dimensions": {
                "Key": "SERVICE",
                "Values": ["Amazon Simple Storage Service"],
            }
        }

    def _is_storage_usage_type(self, usage_type: str) -> bool:
        """Check if USAGE_TYPE is storage-related."""
        return "TimedStorage" in usage_type or "EarlyDelete" in usage_type

    def _ce_summary(self, ce_client, start_date: str, end_date: str) -> Dict:
        return ce_client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["BlendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
            Filter=self._get_service_filter(),
        )

    def _ce_detailed(self, ce_client, start_date: str, end_date: str) -> Optional[Dict]:
        try:
            return ce_client.get_cost_and_usage(
                TimePeriod={"Start": start_date, "End": end_date},
                Granularity="MONTHLY",
                Metrics=["BlendedCost"],
                GroupBy=[
                    {"Type": "DIMENSION", "Key": "RESOURCE_ID"},
                    {"Type": "DIMENSION", "Key": "USAGE_TYPE"}
                ],
                Filter=self._get_service_filter(),
            )
        except Exception:
            return None

    def cost_estimate(
        self,
        bucket_name: str,
        output_dir: str = "./storage_cost_reports",
        periods: Optional[List[str]] = None,
    ) -> str:
        """
        Estimate S3 storage costs for the specified bucket and save a table in a text file.

        Args:
            bucket_name: The S3 bucket to estimate storage costs for.
            output_dir: Directory to save the report.
            periods: List of periods like ["day", "week", "quarter", "year"].

        Returns:
            Path to the generated report file (empty string on failure).
        """
        os.makedirs(output_dir, exist_ok=True)
        periods = periods or list(self.PERIODS)
        ce = self.ce

        all_costs: Dict[str, Dict] = {}
        for p in periods:
            start, end = self._date_range(p)
            try:
                summary = self._ce_summary(ce, start, end)
                detailed = self._ce_detailed(ce, start, end)

                period_key = f"{start} to {end}"

                # Calculate total storage cost from summary
                total_cost = 0.0
                for res in summary.get("ResultsByTime", []):
                    for g in res.get("Groups", []):
                        usage_type = g["Keys"][0]  # Since grouped by USAGE_TYPE
                        cost = float(g["Metrics"]["BlendedCost"]["Amount"])
                        if self._is_storage_usage_type(usage_type):
                            total_cost += cost

                entry = {"total_storage_cost": total_cost, "bucket_breakdown": {}}

                # Bucket breakdown from detailed
                if detailed:
                    bucket_costs = {}
                    other_costs = 0.0
                    for res in detailed.get("ResultsByTime", []):
                        for g in res.get("Groups", []):
                            rid, usage_type = g["Keys"]  # RESOURCE_ID, USAGE_TYPE
                            cost = float(g["Metrics"]["BlendedCost"]["Amount"])
                            if self._is_storage_usage_type(usage_type):
                                if bucket_name in rid:
                                    bucket_costs[bucket_name] = bucket_costs.get(bucket_name, 0.0) + cost
                                else:
                                    other_costs += cost
                    entry["bucket_breakdown"][bucket_name] = bucket_costs.get(bucket_name, 0.0)
                    if other_costs > 0:
                        entry["bucket_breakdown"]["Other"] = other_costs

                all_costs[p] = {period_key: entry}
            except Exception as e:
                print(f"Failed to get storage cost for period '{p}': {e}")
                all_costs[p] = {}

        # Build filename
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(output_dir, f"{bucket_name}_{self._region_name}_{ts}_storage_cost_report.txt")

        # Write report
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(self._render_table_report(bucket_name, all_costs))
            print(f"Storage cost report saved to: {report_path}")
            return report_path
        except Exception as e:
            print(f"Error saving report: {e}")
            return ""

    def _render_table_report(self, bucket_name: str, all_costs: Dict[str, Dict]) -> str:
        lines: List[str] = []
        lines.append("=" * 80)
        lines.append(f"AWS S3 STORAGE COST REPORT FOR BUCKET: {bucket_name}")
        lines.append("=" * 80)
        lines.append(f"Generated (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Region: {self._region_name}")
        lines.append("")

        # Cost Table
        lines.append("STORAGE COST TABLE")
        lines.append("-" * 60)
        lines.append(f"{'Period':<12} {'Date Range':<30} {'Total Cost (USD)':<18}")
        lines.append("-" * 60)
        for p in self.PERIODS:
            if p not in all_costs or not all_costs[p]:
                continue
            period_key, data = list(all_costs[p].items())[0]
            total = data.get("total_storage_cost", 0.0)
            lines.append(f"{p.capitalize():<12} {period_key:<30} ${total:<17.2f}")
        lines.append("")

        # Detailed Breakdown per Period
        for p in self.PERIODS:
            if p not in all_costs or not all_costs[p]:
                continue
            period_key, data = list(all_costs[p].items())[0]
            lines.append(f"DETAILS FOR {p.upper()} [{period_key}]")
            lines.append("-" * 60)
            bd = data.get("bucket_breakdown", {})
            if not bd:
                lines.append("  (No bucket-level breakdown available. Enable resource-level granularity in Cost Explorer.)")
            else:
                lines.append(f"{'Bucket':<30} {'Cost (USD)':<18}")
                lines.append("-" * 50)
                for b, cost in sorted(bd.items(), key=lambda x: x[1], reverse=True):
                    lines.append(f"{b:<30} ${cost:<17.2f}")
            lines.append("")

        lines.append("=" * 80)
        lines.append("Note: Costs are storage-related only (e.g., TimedStorage and EarlyDelete fees) from AWS Cost Explorer.")
        lines.append("      Bucket breakdown requires Cost Explorer resource-level granularity enabled.")
        lines.append("=" * 80)
        return "\n".join(lines)
    