# aws_cost_estimator.py
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import boto3


class S3CostEstimator:
    """
    Estimate AWS S3 costs using AWS Cost Explorer.

    What you get:
      • Totals for S3 by period (day/week/month/quarter/year)
      • Best-effort bucket-level breakdown (requires CE resource granularity)
    """

    PERIODS = ("day", "week", "month", "quarter", "year")

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
        self.s3 = self.session.client("s3")

    # ---------- utilities ----------
    @staticmethod
    def _date_range(period: str) -> tuple[str, str]:
        now = datetime.utcnow()
        if period == "day":
            start = now - timedelta(days=1)
        elif period == "week":
            start = now - timedelta(weeks=1)
        elif period == "month":
            start = now - timedelta(days=30)
        elif period == "quarter":
            start = now - timedelta(days=90)
        elif period == "year":
            start = now - timedelta(days=365)
        else:
            raise ValueError("period must be one of: day, week, month, quarter, year")
        return start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")

    # ---------- CE calls ----------
    def _ce_summary(self, ce_client, start_date: str, end_date: str) -> Dict:
        return ce_client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["BlendedCost", "UnblendedCost", "UsageQuantity"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
            Filter={"Dimensions": {"Key": "SERVICE", "Values": ["Amazon Simple Storage Service"]}},
        )

    def _ce_detailed(self, ce_client, start_date: str, end_date: str) -> Optional[Dict]:
        # Attempts resource-level cost; requires CE resource granularity enabled
        return ce_client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["BlendedCost", "UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "RESOURCE_ID"}],
            Filter={"Dimensions": {"Key": "SERVICE", "Values": ["Amazon Simple Storage Service"]}},
        )

    # ---------- public API ----------
    def cost_estimate(
        self,
        *,
        region: Optional[str] = None,
        bucket_filter: Optional[Union[str, List[str]]] = None,
        output_dir: str = "./cost_reports",
        periods: Optional[List[str]] = None,
    ) -> str:
        """
        Generate an S3 cost report.

        Args:
            region: dynamic region override for this call.
            bucket_filter: a bucket name or list of names. If None, analyze all buckets.
            output_dir: where to write the .txt report.
            periods: subset of ["day","week","month","quarter","year"].

        Returns:
            Path to the generated report ('' on failure).
        """
        # region override (create temp session/clients if needed)
        if region and region != self._region_name:
            creds = self.session.get_credentials()
            tmp_sess = boto3.Session(
                aws_access_key_id=creds.access_key if creds else None,
                aws_secret_access_key=creds.secret_key if creds else None,
                aws_session_token=getattr(creds, "token", None) if creds else None,
                region_name=region,
            )
            ce = tmp_sess.client("ce")
            s3 = tmp_sess.client("s3")
        else:
            ce = self.ce
            s3 = self.s3
            region = self._region_name

        os.makedirs(output_dir, exist_ok=True)

        # Resolve buckets in scope
        try:
            if bucket_filter is None:
                buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
            else:
                if isinstance(bucket_filter, str):
                    buckets = [bucket_filter]
                else:
                    buckets = list(bucket_filter)
            if not buckets:
                print("No S3 buckets in scope.")
                return ""
        except Exception as e:
            print(f"Error resolving bucket list: {e}")
            return ""

        periods = periods or list(self.PERIODS)

        # Collect cost data
        all_costs: Dict[str, Dict] = {}
        for p in periods:
            start, end = self._date_range(p)
            try:
                summary = self._ce_summary(ce, start, end)
                try:
                    detailed = self._ce_detailed(ce, start, end)
                except Exception:
                    detailed = None

                # Summarize totals
                period_key = f"{start}_to_{end}"
                total_cost = 0.0
                for res in summary.get("ResultsByTime", []):
                    for g in res.get("Groups", []):
                        total_cost += float(g["Metrics"]["BlendedCost"]["Amount"])

                entry = {"total_s3_cost": total_cost, "bucket_breakdown": {}}

                # Best-effort bucket attribution
                if detailed:
                    for res in detailed.get("ResultsByTime", []):
                        for g in res.get("Groups", []):
                            rid = g.get("Keys", [""])[0]
                            cost = float(g["Metrics"]["BlendedCost"]["Amount"])
                            matched = None
                            for b in buckets:
                                if b in rid:
                                    matched = b
                                    break
                            if matched is None:
                                matched = "Other S3 Costs"
                            entry["bucket_breakdown"][matched] = entry["bucket_breakdown"].get(matched, 0.0) + cost

                all_costs[p] = {period_key: entry}
            except Exception as e:
                print(f"Failed to get cost for period '{p}': {e}")
                all_costs[p] = {}

        # Build filename
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        name_part = (
            buckets[0] if len(buckets) == 1 else f"{buckets[0]}_{buckets[1]}_and_{max(len(buckets)-2,0)}_more"
        )
        report_path = os.path.join(output_dir, f"{name_part}_{region}_{ts}_cost_report.txt")

        # Write report
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(self._render_report(region, buckets, all_costs))
            print(f"Cost report saved to: {report_path}")
            return report_path
        except Exception as e:
            print(f"Error saving report: {e}")
            return ""

    def _render_report(self, region: str, buckets: List[str], all_costs: Dict[str, Dict]) -> str:
        lines: List[str] = []
        lines.append("=" * 80)
        lines.append("AWS S3 COST ESTIMATION REPORT")
        lines.append("=" * 80)
        lines.append(f"Generated (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"AWS Region: {region}")
        lines.append(f"Buckets in scope ({len(buckets)}):")
        for i, b in enumerate(buckets, 1):
            lines.append(f"  {i:2d}. {b}")
        lines.append("")

        # Summary by period
        lines.append("COST SUMMARY BY PERIOD")
        lines.append("-" * 60)
        lines.append(f"{'Period':<12} {'Total Cost (USD)':<18} {'Date Range':<30}")
        lines.append("-" * 60)
        for p in self.PERIODS:
            if p not in all_costs or not all_costs[p]:
                continue
            (period_key, data), = all_costs[p].items()
            total = data.get("total_s3_cost", 0.0)
            lines.append(f"{p.capitalize():<12} ${total:<17.2f} {period_key.replace('_to_', ' to ')}")
        lines.append("")

        # Detailed per-period
        for p in self.PERIODS:
            if p not in all_costs or not all_costs[p]:
                continue
            (period_key, data), = all_costs[p].items()
            lines.append(f"DETAILS — {p.upper()}  [{period_key.replace('_to_', ' to ')}]")
            lines.append("-" * 60)
            bd = data.get("bucket_breakdown", {})
            if not bd:
                lines.append("  (No resource-level breakdown available. Enable CE resource granularity for S3.)")
            else:
                for bucket, cost in sorted(bd.items(), key=lambda x: x[1], reverse=True):
                    lines.append(f"  - {bucket}: ${cost:.2f}")
            lines.append("")

        lines.append("=" * 80)
        lines.append("Note: Costs come from AWS Cost Explorer and can include storage, requests,")
        lines.append("      and data transfer. Bucket mapping depends on CE configuration.")
        lines.append("=" * 80)
        return "\n".join(lines)


# ---- convenience function for easy imports from other scripts ----
def estimate_s3_cost_report(
    *,
    region: str,
    bucket_name: Optional[Union[str, List[str]]] = None,
    output_dir: str = "./cost_reports",
    periods: Optional[List[str]] = None,
) -> str:
    """
    Convenience wrapper so callers can do:
      from aws_cost_estimator import estimate_s3_cost_report
      estimate_s3_cost_report(region="us-east-2", bucket_name="my-bucket")
    """
    est = S3CostEstimator(region_name=region)
    return est.cost_estimate(region=region, bucket_filter=bucket_name, output_dir=output_dir, periods=periods)
