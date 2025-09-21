# s3_list_objects.py

import boto3
import botocore
from botocore.config import Config
from botocore import UNSIGNED
from typing import List, Optional, Tuple
import os
import argparse
import sys

AuthMode = str  # "auto" | "signed" | "public"

def make_session(profile: Optional[str] = None, region: Optional[str] = None) -> boto3.Session:
    if profile:
        os.environ["AWS_PROFILE"] = profile  # respected by boto3
    return boto3.Session(region_name=region)

def make_s3_client(session: Optional[boto3.Session], region: Optional[str], auth_mode: AuthMode):
    cfg = Config(retries={"max_attempts": 10, "mode": "adaptive"})
    session = session or boto3.Session()
    if auth_mode == "public":
        return session.client("s3", region_name=region, config=cfg.merge(Config(signature_version=UNSIGNED)))
    return session.client("s3", region_name=region, config=cfg)

def check_aws_credentials(session: Optional[boto3.Session] = None) -> bool:
    """
    Validate AWS credentials for signed mode (skip when public).
    """
    try:
        session = session or boto3.Session()
        credentials = session.get_credentials()
        if credentials is None:
            print("❌ No AWS credentials found!")
            print("Configure credentials via one of:")
            print("  1) aws configure")
            print("  2) Environment vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
            print("  3) IAM role (on EC2/Lambda/ECS)")
            return False

        sts = session.client("sts", config=Config(retries={"max_attempts": 5, "mode": "adaptive"}))
        ident = sts.get_caller_identity()
        acct = ident.get("Account", "unknown")
        arn = ident.get("Arn", "unknown")
        print("✅ AWS credentials loaded")
        print(f"   Account: {acct}")
        print(f"   Caller : {arn}")
        return True
    except Exception as e:
        print(f"❌ Error checking credentials: {e}")
        return False

def check_bucket_exists_signed(bucket_name: str, region: Optional[str], session: Optional[boto3.Session]) -> bool:
    """
    Signed check: head_bucket (requires permission).
    """
    try:
        s3 = make_s3_client(session, region, auth_mode="signed")
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' exists and is accessible (signed)")
        return True
    except botocore.exceptions.ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('404', 'NoSuchBucket'):
            print(f"❌ Bucket '{bucket_name}' does not exist")
        elif code in ('403', 'AccessDenied'):
            print(f"❌ Access denied to bucket '{bucket_name}' (signed)")
            print("   Required permissions:")
            print("   - s3:ListBucket (on the bucket)")
            print("   - s3:GetObject  (for reading objects)")
        else:
            print(f"❌ Error accessing bucket '{bucket_name}': {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error checking bucket (signed): {e}")
        return False

def check_bucket_exists_public(bucket_name: str, region: Optional[str], session: Optional[boto3.Session]) -> bool:
    """
    Public check: try an unsigned list with MaxKeys=1.
    If we can list, the bucket exists and is publicly listable or readable.
    """
    try:
        s3 = make_s3_client(session, region, auth_mode="public")
        resp = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        # If no error, bucket exists (may be empty)
        print(f"✅ Bucket '{bucket_name}' reachable without credentials (public)")
        return True
    except botocore.exceptions.ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('AccessDenied', '403'):
            print(f"❌ Public access denied for bucket '{bucket_name}'")
            print("   This bucket may be private or block public list access.")
        elif code in ('NoSuchBucket', '404'):
            print(f"❌ Bucket '{bucket_name}' does not exist")
        else:
            print(f"❌ AWS Error (public): {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error checking bucket (public): {e}")
        return False

def list_all_buckets(session: Optional[boto3.Session] = None) -> List[str]:
    """
    List all accessible S3 buckets (signed only).
    """
    try:
        s3 = make_s3_client(session, region=None, auth_mode="signed")
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        print(f"✅ Found {len(buckets)} accessible buckets")
        return buckets
    except botocore.exceptions.ClientError as e:
        print(f"❌ Error listing buckets: {e}")
        return []
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []

def list_s3_objects(bucket_name: str, prefix: str = "", region: Optional[str] = None,
                    session: Optional[boto3.Session] = None, auth_mode: AuthMode = "signed") -> List[str]:
    """
    List objects with chosen auth mode. In 'auto' we try signed, then public on AccessDenied.
    """
    def _list(mode: AuthMode) -> Tuple[List[str], Optional[str]]:
        try:
            s3 = make_s3_client(session, region, auth_mode=mode)
            paginator = s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            objs: List[str] = []
            count = 0
            for page in page_iterator:
                for obj in page.get("Contents", []):
                    objs.append(obj["Key"])
                    count += 1
            return objs, None
        except botocore.exceptions.ClientError as e:
            code = e.response.get('Error', {}).get('Code')
            return [], code
        except Exception as e:
            return [], f"Unexpected:{e}"

    if auth_mode == "auto":
        # Try signed first
        objects, err = _list("signed")
        if err in (None,):
            pass
        elif err in ("AccessDenied", "403"):
            print("ℹ️ Signed access denied — retrying with public/unsigned...")
            objects, err = _list("public")
        else:
            if err:
                print(f"❌ AWS Error (auto/signed): {err}")
            return []
    else:
        objects, err = _list(auth_mode)
        if err:
            print(f"❌ AWS Error ({auth_mode}): {err}")
            return []

    if not objects:
        if prefix:
            print(f"📂 No objects found in bucket '{bucket_name}' with prefix '{prefix}'")
        else:
            print(f"📂 No objects found in bucket '{bucket_name}'")
    else:
        where = f" (prefix='{prefix}')" if prefix else ""
        mode_label = "public" if auth_mode == "public" else "signed" if auth_mode == "signed" else "auto"
        print(f"📂 Found {len(objects)} objects in bucket '{bucket_name}'{where} [{mode_label}]")
    return objects

def resolve_bucket(session: boto3.Session, *, cli_bucket: Optional[str], env_bucket: Optional[str],
                   non_interactive: bool, auth_mode: AuthMode) -> Tuple[Optional[str], List[str]]:
    """
    Resolve the bucket to use.
    In public mode, we cannot list account buckets, so we only use CLI/env/manual input.
    """
    if cli_bucket:
        return cli_bucket, []

    if env_bucket:
        print(f"ℹ️ Using bucket from env AWS_S3_BUCKET={env_bucket}")
        return env_bucket, []

    if auth_mode == "public":
        # Can't list buckets in public mode; ask user (unless non-interactive).
        if non_interactive:
            return None, []
        while True:
            choice = input("\n🔸 Enter public bucket name: ").strip()
            if choice:
                return choice, []
            print("Please enter a valid bucket name.")
    # signed/auto: we can list
    buckets = list_all_buckets(session)
    if not buckets:
        return None, []

    if non_interactive:
        return None, buckets

    print("\n📋 Available buckets:")
    for i, b in enumerate(buckets, 1):
        print(f"  {i:2d}. {b}")

    while True:
        choice = input("\n🔸 Enter bucket number to use (or type a name manually): ").strip()
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(buckets):
                return buckets[idx - 1], buckets
            print("Invalid number. Try again.")
        elif choice:
            return choice, buckets
        else:
            print("Please enter a valid selection.")

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="List objects in an S3 bucket (supports public/unsigned access).")
    p.add_argument("--bucket", "-b", help="Bucket name (overrides env and interactive selection)")
    p.add_argument("--prefix", "-p", default="", help="Prefix to filter objects (e.g., 'logs/2025/')")
    p.add_argument("--region", "-r", help="AWS region to use for S3 client")
    p.add_argument("--profile", help="AWS named profile to use (e.g., 'dev')")
    p.add_argument("--max-show", type=int, default=20, help="How many objects to show in the summary (default: 20)")
    p.add_argument("--non-interactive", action="store_true",
                   help="Fail if no bucket provided (no interactive picker)")
    p.add_argument("--auth", choices=["auto", "signed", "public"], default="auto",
                   help="Access mode: auto (try signed then public), signed (IAM), public (unsigned only)")
    return p.parse_args()

def main():
    print("🔍 AWS S3 Object Lister")
    print("=" * 40)

    args = parse_args()
    session = make_session(profile=args.profile, region=args.region)

    # Step 1: Credentials (skip in public mode)
    if args.auth != "public":
        if not check_aws_credentials(session):
            if args.auth == "signed":
                sys.exit(1)
            else:
                print("ℹ️ Falling back to public access where possible.")
    else:
        print("ℹ️ Public/unsigned mode: skipping credential checks.")

    # Step 2/3: Resolve bucket
    env_bucket = os.getenv("AWS_S3_BUCKET")
    bucket, known_buckets = resolve_bucket(session, cli_bucket=args.bucket, env_bucket=env_bucket,
                                           non_interactive=args.non_interactive, auth_mode=args.auth)

    if not bucket:
        print("❌ No bucket specified and none selected.")
        if known_buckets:
            print("\n💡 Suggestion: Use one of these buckets with '--bucket':")
            for i, b in enumerate(known_buckets[:5], 1):
                print(f"   {i}. {b}")
        sys.exit(1)

    print(f"\n🎯 Target bucket: '{bucket}'")

    # Step 3.5: Bucket existence/access check based on mode
    exists = False
    if args.auth == "signed":
        exists = check_bucket_exists_signed(bucket, region=args.region, session=session)
    elif args.auth == "public":
        exists = check_bucket_exists_public(bucket, region=args.region, session=session)
    else:  # auto
        if check_bucket_exists_signed(bucket, region=args.region, session=session):
            exists = True
        else:
            print("ℹ️ Trying public/unsigned reachability...")
            exists = check_bucket_exists_public(bucket, region=args.region, session=session)

    if not exists:
        if known_buckets:
            print(f"\n💡 Suggestion: Try one of your accessible buckets:")
            for i, b in enumerate(known_buckets[:5], 1):
                print(f"   {i}. {b}")
        sys.exit(1)

    # Step 4: List objects
    print(f"\n📁 Listing objects in '{bucket}'...")
    files = list_s3_objects(bucket, prefix=args.prefix, region=args.region, session=session, auth_mode=args.auth)

    if files:
        show_n = max(0, args.max_show)
        print(f"\n📄 Objects in bucket (showing first {min(len(files), show_n)}):")
        for i, f in enumerate(files[:show_n], 1):
            print(f"   {i:2d}. {f}")
        if len(files) > show_n:
            print(f"   ... and {len(files) - show_n} more objects")

    print("\n✅ Script completed successfully!")

if __name__ == "__main__":
    main()
