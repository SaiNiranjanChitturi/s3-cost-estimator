# AWS S3 Storage Cost Estimator

A lightweight Python library to estimate **Amazon S3 storage costs** using the AWS Cost Explorer API.  
It focuses only on **storage-related charges** (`TimedStorage`, `EarlyDelete` fees) and generates clear text reports with cost breakdowns.

---

## 📌 Features

- Estimate **S3 storage costs** for a specific bucket
- Supports multiple reporting periods:
  - Daily
  - Weekly
  - Quarterly
  - Annual
- Produces clean **text-based reports** with tables and breakdowns
- Saves reports with timestamps in an output directory
- Supports AWS Cost Explorer **resource-level granularity** (if enabled)

---

## 📂 Project Structure

```
.
├── S3StorageCostEstimator.py   # Core library
├── example.py                  # Example usage
└── README.md                   # Documentation
```

---

## ⚙️ Installation

Clone this repository and install dependencies:

```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
pip install boto3
```

Make sure your AWS credentials are configured via one of:

- `aws configure` (recommended)
- Environment variables:  
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- IAM Role (if running on AWS services)

---

## 🚀 Usage

### Run the Example

```bash
python example.py
```

This will:

- Estimate costs for the bucket defined in `example.py`  
- Generate a report in `./storage_cost_reports/`  
- Print the report path to the console

---

### Example Report Output

```
================================================================================
AWS S3 STORAGE COST REPORT FOR BUCKET: my-example-bucket
================================================================================
Generated (UTC): 2025-09-21 22:45:11
Region: us-east-1

STORAGE COST TABLE
------------------------------------------------------------
Period       Date Range                    Total Cost (USD)
------------------------------------------------------------
Day          2025-09-20 to 2025-09-21      $1.23
Week         2025-09-14 to 2025-09-21      $8.45
Quarter      2025-06-23 to 2025-09-21      $34.12
Year         2024-09-21 to 2025-09-21      $120.55

DETAILS FOR DAY [2025-09-20 to 2025-09-21]
------------------------------------------------------------
Bucket                         Cost (USD)
------------------------------------------------------------
my-example-bucket              $1.23
Other                          $0.00

================================================================================
Note: Costs are storage-related only (e.g., TimedStorage and EarlyDelete fees).
      Bucket breakdown requires Cost Explorer resource-level granularity enabled.
================================================================================
```

---

## 🔧 Custom Usage

You can import and use the library in your own script:

```python
from S3StorageCostEstimator import S3StorageCostEstimator

# Initialize estimator (default region is us-east-1)
estimator = S3StorageCostEstimator(region_name="us-east-1")

# Estimate storage costs
report_path = estimator.cost_estimate(bucket_name="my-example-bucket")

print(f"Report saved at: {report_path}")
```

### Parameters

- `bucket_name` (str): S3 bucket to analyze  
- `output_dir` (str, optional): Directory for reports (default: `./storage_cost_reports`)  
- `periods` (list[str], optional): Periods to calculate (default: all: `day`, `week`, `quarter`, `year`)  

---

## 📦 Requirements

- Python 3.8+
- boto3
- AWS account with Cost Explorer enabled

---

## 📝 License

This project is licensed under the [MIT License](./LICENSE).
