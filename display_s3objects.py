# s3_list_objects.py

import boto3
import botocore
from typing import List, Optional
import os

def check_aws_credentials():
    """
    Check if AWS credentials are properly configured
    """
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        
        if credentials is None:
            print("❌ No AWS credentials found!")
            print("Please configure AWS credentials using one of these methods:")
            print("1. AWS CLI: aws configure")
            print("2. Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
            print("3. IAM roles (if running on EC2)")
            return False
        
        print("✅ AWS credentials found")
        print(f"   Access Key ID: {credentials.access_key[:10]}...")
        return True
        
    except Exception as e:
        print(f"❌ Error checking credentials: {e}")
        return False

def check_bucket_exists(bucket_name: str, region: Optional[str] = None) -> bool:
    """
    Check if the bucket exists and is accessible
    """
    try:
        if region:
            s3 = boto3.client("s3", region_name=region)
        else:
            s3 = boto3.client("s3")
            
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' exists and is accessible")
        return True
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"❌ Bucket '{bucket_name}' does not exist")
        elif error_code == '403':
            print(f"❌ Access denied to bucket '{bucket_name}'")
            print("   Check if your AWS credentials have the required permissions:")
            print("   - s3:ListBucket")
            print("   - s3:GetObject")
        else:
            print(f"❌ Error accessing bucket '{bucket_name}': {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error checking bucket: {e}")
        return False

def list_all_buckets() -> List[str]:
    """
    List all accessible S3 buckets for debugging
    """
    try:
        s3 = boto3.client("s3")
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        
        print(f"✅ Found {len(buckets)} accessible buckets:")
        for i, bucket in enumerate(buckets, 1):
            print(f"   {i}. {bucket}")
        
        return buckets
        
    except botocore.exceptions.ClientError as e:
        print(f"❌ Error listing buckets: {e}")
        return []
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []

def list_s3_objects(bucket_name: str, prefix: str = "", region: Optional[str] = None) -> List[str]:
    """
    List all objects in an S3 bucket with improved error handling.
    
    :param bucket_name: Name of the S3 bucket
    :param prefix: Optional prefix to filter objects
    :param region: Optional AWS region
    :return: List of object keys
    """
    try:
        if region:
            s3 = boto3.client("s3", region_name=region)
        else:
            s3 = boto3.client("s3")
            
        # Use paginator for large buckets
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        objects = []
        object_count = 0
        
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects.append(obj["Key"])
                    object_count += 1
        
        if object_count == 0:
            print(f"📂 No objects found in bucket '{bucket_name}' with prefix '{prefix}'")
        else:
            print(f"📂 Found {object_count} objects in bucket '{bucket_name}'")
            
        return objects
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            print(f"❌ Access denied to bucket '{bucket_name}'")
            print("   Required permissions: s3:ListBucket, s3:GetObject")
        elif error_code == 'NoSuchBucket':
            print(f"❌ Bucket '{bucket_name}' does not exist")
        else:
            print(f"❌ AWS Error: {e}")
        return []
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []

if __name__ == "__main__":
    print("🔍 AWS S3 Object Lister")
    print("=" * 40)
    
    # Step 1: Check AWS credentials
    if not check_aws_credentials():
        exit(1)
    
    # Step 2: List all available buckets for reference
    print("\n📋 Available buckets:")
    available_buckets = list_all_buckets()
    
    if not available_buckets:
        print("❌ No buckets found. Please check your AWS credentials and permissions.")
        exit(1)
    
    # Step 3: Try to access the specified bucket
    bucket = "srivinaya-bucket"
    prefix = ""  # set if you want to filter by folder path
    
    print(f"\n🎯 Target bucket: '{bucket}'")
    
    # Check if bucket exists and is accessible
    if not check_bucket_exists(bucket):
        print(f"\n💡 Suggestion: Try one of your accessible buckets:")
        for i, available_bucket in enumerate(available_buckets[:5], 1):
            print(f"   {i}. {available_bucket}")
        exit(1)
    
    # Step 4: List objects in the bucket
    print(f"\n📁 Listing objects in '{bucket}'...")
    files = list_s3_objects(bucket, prefix)
    
    if files:
        print(f"\n📄 Objects in bucket (showing first 20):")
        for i, f in enumerate(files[:20], 1):
            print(f"   {i:2d}. {f}")
        
        if len(files) > 20:
            print(f"   ... and {len(files) - 20} more objects")
    
    print("\n✅ Script completed successfully!")
