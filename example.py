from S3StorageCostEstimator import S3StorageCostEstimator

def main():
    # Initialize the estimator with the default region (us-east-1), adjust if the bucket is in a different region
    estimator = S3StorageCostEstimator(region_name="us-east-1")
    
    # Calculate the storage cost for the 'aws-bigdata-blog' bucket
    # This will generate a text file with a table of daily/weekly/quarterly/annual costs
    report_path = estimator.cost_estimate(bucket_name="793861635529-cost-estimator")
    
    if report_path:
        print(f"Cost report generated at: {report_path}")
    else:
        print("Failed to generate cost report.")

if __name__ == "__main__":
    main()