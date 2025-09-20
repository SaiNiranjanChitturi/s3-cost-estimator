# list_s3_and_estimate.py
import argparse
import sys
from typing import Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Import the helper exactly as requested
from aws_cost_estimator import estimate_s3_cost_report


def get_s3_client(region: str):
    return boto3.client("s3", region_name=region)


def list_all_buckets(s3) -> List[str]:
    try:
        resp = s3.list_buckets()
        return [b["Name"] for b in resp.get("Buckets", [])]
    except (BotoCoreError, ClientError) as e:
        print(f"Error listing buckets: {e}", file=sys.stderr)
        return []


def list_bucket_objects(
    s3,
    bucket: str,
    prefix: str = "",
    max_keys: Optional[int] = 100,
    fetch_all: bool = False,
) -> Iterable[Tuple[str, int]]:
    """
    Yields (key, size_bytes) for objects in a bucket.
    Uses a paginator; if fetch_all is False, stops after max_keys items.
    """
    paginator = s3.get_paginator("list_objects_v2")
    params = {"Bucket": bucket}
    if prefix:
        params["Prefix"] = prefix

    count = 0
    try:
        for page in paginator.paginate(**params):
            for obj in page.get("Contents", []) or []:
                yield obj["Key"], int(obj.get("Size", 0))
                count += 1
                if not fetch_all and max_keys is not None and count >= max_keys:
                    return
    except (BotoCoreError, ClientError) as e:
        print(f"Error listing objects for bucket '{bucket}': {e}", file=sys.stderr)


def human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    for u in units:
        if size < 1024.0:
            return f"{size:.2f} {u}"
        size /= 1024.0
    return f"{size:.2f} EB"


def print_bucket_inventory(s3, bucket: str, prefix: str, max_keys: Optional[int], fetch_all: bool):
    print(f"\nBucket: {bucket}")
    print("-" * (8 + len(bucket)))
    total = 0
    shown = 0
    for key, sz in list_bucket_objects(s3, bucket, prefix=prefix, max_keys=max_keys, fetch_all=fetch_all):
        print(f"  - {key}  ({human_size(sz)})")
        total += sz
        shown += 1
    print(f"Items shown: {shown}{' (all)' if fetch_all else ''}")
    print(f"Approx size (shown): {human_size(total)}")


def generate_cost_report(region: str, bucket_names: List[str], outdir: str) -> Optional[str]:
    """
    Calls your library helper exactly as designed:
    estimate_s3_cost_report(region=<region>, bucket_name=<str|list>, output_dir=<dir>)
    """
    try:
        bucket_param = bucket_names[0] if len(bucket_names) == 1 else bucket_names
        report_path = estimate_s3_cost_report(region=region, bucket_name=bucket_param, output_dir=outdir)
        if report_path:
            print(f"\n✅ Cost report generated: {report_path}")
        else:
            print("\n❌ Failed to generate cost report (see logs above).")
        return report_path
    except Exception as e:
        print(f"\n❌ Error generating cost report: {e}", file=sys.stderr)
        return None


def parse_args():
    p = argparse.ArgumentParser(
        description="List S3 buckets and contents, then generate S3 cost report via aws_cost_estimator.py"
    )
    # make region optional with default
    p.add_argument(
        "--region",
        default="us-east-1",     # 👈 default region set here
        help="AWS region (default: us-east-1)",
    )
    p.add_argument(
        "--bucket",
        action="append",
        help="Bucket to inspect (can be specified multiple times). If omitted, all buckets are listed and included in the report.",
    )
    p.add_argument("--prefix", default="", help="Optional key prefix to filter objects")
    p.add_argument("--max-keys", type=int, default=100, help="Max objects to list per bucket (ignored with --all)")
    p.add_argument("--all", action="store_true", help="List all objects (may be slow)")
    p.add_argument("--report-dir", default="./cost_reports", help="Directory to save the cost report")
    return p.parse_args()



def main():
    args = parse_args()
    region = args.region
    s3 = get_s3_client(region)

    # Resolve buckets in scope
    if args.bucket:
        buckets = args.bucket
    else:
        buckets = list_all_buckets(s3)
        if not buckets:
            print("No buckets found or access denied.")
            sys.exit(1)

    print(f"AWS Region: {region}")
    print(f"Buckets in scope ({len(buckets)}):")
    for b in buckets:
        print(f" - {b}")

    for b in buckets:
        print_bucket_inventory(
            s3,
            bucket=b,
            prefix=args.prefix,
            max_keys=None if args.all else args.max_keys,
            fetch_all=args.all,
        )

    # Generate cost report using the library
    generate_cost_report(region=region, bucket_names=buckets, outdir=args.report_dir)


if __name__ == "__main__":
    main()
