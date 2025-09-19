import boto3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os


class S3CostEstimator:
    """
    A library to estimate AWS S3 costs using Cost Explorer API
    """
    
    def __init__(self, aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None, 
                 region_name: str = 'us-east-1'):
        """
        Initialize the S3 Cost Estimator
        
        Args:
            aws_access_key_id: AWS access key (optional, can use environment variables)
            aws_secret_access_key: AWS secret key (optional, can use environment variables)
            region_name: AWS region name
        """
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        
        self.cost_explorer = self.session.client('ce')
        self.s3_client = self.session.client('s3')
    
    def get_s3_buckets_with_client(self, s3_client) -> List[str]:
        """
        Get list of all S3 buckets using specified client
        
        Args:
            s3_client: S3 client to use
            
        Returns:
            List of bucket names
        """
        try:
            response = s3_client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except Exception as e:
            print(f"Error fetching S3 buckets: {e}")
            return []
    
    def get_s3_costs_by_bucket_with_client(self, cost_explorer, start_date: str, end_date: str) -> Dict:
        """
        Get S3 costs grouped by bucket using specified cost explorer client
        
        Args:
            cost_explorer: Cost Explorer client to use
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary with bucket costs
        """
        try:
            response = cost_explorer.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date,
                    'End': end_date
                },
                Granularity='MONTHLY',
                Metrics=['BlendedCost', 'UnblendedCost', 'UsageQuantity'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'LINKED_ACCOUNT'
                    },
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ],
                Filter={
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': ['Amazon Simple Storage Service']
                    }
                }
            )
            
            # Try to get more detailed breakdown by resource
            detailed_response = cost_explorer.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date,
                    'End': end_date
                },
                Granularity='MONTHLY',
                Metrics=['BlendedCost', 'UnblendedCost'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'RESOURCE_ID'
                    }
                ],
                Filter={
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': ['Amazon Simple Storage Service']
                    }
                }
            )
            
            return {
                'summary': response,
                'detailed': detailed_response
            }
            
        except Exception as e:
            print(f"Error fetching cost data: {e}")
            return {}
    
    def get_s3_buckets(self) -> List[str]:
        """
        Get list of all S3 buckets in the account
        
        Returns:
            List of bucket names
        """
        try:
            response = self.s3_client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except Exception as e:
            print(f"Error fetching S3 buckets: {e}")
            return []
    
    def get_date_range(self, period: str) -> tuple:
        """
        Get start and end dates for the specified period
        
        Args:
            period: 'day', 'week', 'month', 'quarter', 'year'
            
        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        end_date = datetime.now()
        
        if period == 'day':
            start_date = end_date - timedelta(days=1)
        elif period == 'week':
            start_date = end_date - timedelta(weeks=1)
        elif period == 'month':
            start_date = end_date - timedelta(days=30)
        elif period == 'quarter':
            start_date = end_date - timedelta(days=90)
        elif period == 'year':
            start_date = end_date - timedelta(days=365)
        else:
            raise ValueError("Period must be one of: day, week, month, quarter, year")
        
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    
    def get_s3_costs_by_bucket(self, start_date: str, end_date: str) -> Dict:
        """
        Get S3 costs grouped by bucket for the specified date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary with bucket costs
        """
        try:
            response = self.cost_explorer.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date,
                    'End': end_date
                },
                Granularity='MONTHLY',
                Metrics=['BlendedCost', 'UnblendedCost', 'UsageQuantity'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'LINKED_ACCOUNT'
                    },
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ],
                Filter={
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': ['Amazon Simple Storage Service']
                    }
                }
            )
            
            # Try to get more detailed breakdown by resource
            detailed_response = self.cost_explorer.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date,
                    'End': end_date
                },
                Granularity='MONTHLY',
                Metrics=['BlendedCost', 'UnblendedCost'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'RESOURCE_ID'
                    }
                ],
                Filter={
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': ['Amazon Simple Storage Service']
                    }
                }
            )
            
            return {
                'summary': response,
                'detailed': detailed_response
            }
            
        except Exception as e:
            print(f"Error fetching cost data: {e}")
            return {}
    
    def format_cost_data(self, cost_data: Dict, buckets: List[str]) -> Dict:
        """
        Format cost data for display
        
        Args:
            cost_data: Raw cost data from Cost Explorer
            buckets: List of S3 bucket names
            
        Returns:
            Formatted cost dictionary
        """
        formatted_data = {}
        
        if not cost_data or 'summary' not in cost_data:
            return formatted_data
        
        # Process summary data
        for result in cost_data['summary'].get('ResultsByTime', []):
            period = result.get('TimePeriod', {})
            start = period.get('Start', 'Unknown')
            end = period.get('End', 'Unknown')
            
            total_cost = 0
            for group in result.get('Groups', []):
                cost = float(group.get('Metrics', {}).get('BlendedCost', {}).get('Amount', 0))
                total_cost += cost
            
            formatted_data[f"{start}_to_{end}"] = {
                'total_s3_cost': total_cost,
                'bucket_breakdown': {}
            }
        
        # Try to match detailed data to buckets
        if 'detailed' in cost_data:
            for result in cost_data['detailed'].get('ResultsByTime', []):
                period = result.get('TimePeriod', {})
                period_key = f"{period.get('Start', 'Unknown')}_to_{period.get('End', 'Unknown')}"
                
                if period_key in formatted_data:
                    for group in result.get('Groups', []):
                        resource_id = group.get('Keys', ['Unknown'])[0]
                        cost = float(group.get('Metrics', {}).get('BlendedCost', {}).get('Amount', 0))
                        
                        # Try to extract bucket name from resource ID
                        bucket_name = 'Other S3 Costs'
                        for bucket in buckets:
                            if bucket in resource_id:
                                bucket_name = bucket
                                break
                        
                        if bucket_name not in formatted_data[period_key]['bucket_breakdown']:
                            formatted_data[period_key]['bucket_breakdown'][bucket_name] = 0
                        
                        formatted_data[period_key]['bucket_breakdown'][bucket_name] += cost
        
        return formatted_data
    
    def Cost_Estimate(self, output_dir: str = './cost_reports', region: Optional[str] = None) -> str:
        """
        Main method to generate cost estimates for S3 buckets
        
        Args:
            output_dir: Directory to save the output file
            region: AWS region to use for this specific operation (overrides instance region)
            
        Returns:
            Path to the generated report file
        """
        # Use specified region or fall back to instance region
        if region:
            # Create temporary clients for the specified region
            temp_session = boto3.Session(
                aws_access_key_id=self.session.get_credentials().access_key,
                aws_secret_access_key=self.session.get_credentials().secret_key,
                aws_session_token=self.session.get_credentials().token,
                region_name=region
            )
            cost_explorer = temp_session.client('ce')
            s3_client = temp_session.client('s3')
        else:
            cost_explorer = self.cost_explorer
            s3_client = self.s3_client
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get S3 buckets
        buckets = self.get_s3_buckets_with_client(s3_client)
        if not buckets:
            print("No S3 buckets found or error accessing buckets")
            return ""
        
        # Prepare data for all time periods
        periods = ['day', 'week', 'month', 'quarter', 'year']
        all_costs = {}
        
        for period in periods:
            start_date, end_date = self.get_date_range(period)
            cost_data = self.get_s3_costs_by_bucket_with_client(cost_explorer, start_date, end_date)
            all_costs[period] = self.format_cost_data(cost_data, buckets)
        
        # Generate filename with timestamp and region info
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        bucket_list = '_'.join(buckets[:3])  # Use first 3 buckets in filename
        if len(buckets) > 3:
            bucket_list += f'_and_{len(buckets)-3}_more'
        
        region_info = region if region else self.session.region_name
        filename = f"{bucket_list}_{region_info}_{timestamp}_cost_report.txt"
        filepath = os.path.join(output_dir, filename)
        
        # Generate report content
        region_info = region if region else self.session.region_name
        report_content = self.generate_report_content(all_costs, buckets, region_info)
        
        # Save to file
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(report_content)
            print(f"Cost report saved to: {filepath}")
            return filepath
        except Exception as e:
            print(f"Error saving report: {e}")
            return ""
    
    def generate_report_content(self, all_costs: Dict, buckets: List[str], region: str) -> str:
        """
        Generate formatted report content
        
        Args:
            all_costs: Dictionary containing cost data for all periods
            buckets: List of S3 bucket names
            region: AWS region used for the analysis
            
        Returns:
            Formatted report string
        """
        report = []
        report.append("="*80)
        report.append("AWS S3 COST ESTIMATION REPORT")
        report.append("="*80)
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"AWS Region: {region}")
        report.append(f"Total buckets analyzed: {len(buckets)}")
        report.append("")
        
        report.append("BUCKETS IN SCOPE:")
        report.append("-" * 20)
        for i, bucket in enumerate(buckets, 1):
            report.append(f"{i:2d}. {bucket}")
        report.append("")
        
        # Summary table
        report.append("COST SUMMARY BY PERIOD")
        report.append("-" * 25)
        report.append(f"{'Period':<12} {'Total Cost (USD)':<18} {'Date Range':<25}")
        report.append("-" * 60)
        
        periods = ['day', 'week', 'month', 'quarter', 'year']
        for period in periods:
            if period in all_costs:
                total_cost = 0
                date_range = "N/A"
                
                for period_key, data in all_costs[period].items():
                    total_cost += data.get('total_s3_cost', 0)
                    if period_key != 'total':
                        date_range = period_key.replace('_to_', ' to ')
                
                report.append(f"{period.capitalize():<12} ${total_cost:<17.2f} {date_range}")
        
        report.append("")
        
        # Detailed breakdown by period
        for period in periods:
            if period not in all_costs:
                continue
                
            report.append(f"DETAILED BREAKDOWN - {period.upper()}")
            report.append("-" * 40)
            
            for period_key, data in all_costs[period].items():
                if period_key == 'total':
                    continue
                    
                report.append(f"Period: {period_key.replace('_to_', ' to ')}")
                report.append(f"Total S3 Cost: ${data.get('total_s3_cost', 0):.2f}")
                
                bucket_breakdown = data.get('bucket_breakdown', {})
                if bucket_breakdown:
                    report.append("Breakdown by resource:")
                    for resource, cost in bucket_breakdown.items():
                        report.append(f"  - {resource}: ${cost:.2f}")
                else:
                    report.append("  (No detailed breakdown available)")
                report.append("")
        
        report.append("="*80)
        report.append("NOTE: Cost data is retrieved from AWS Cost Explorer API.")
        report.append("Costs may include storage, requests, and data transfer charges.")
        report.append("Detailed bucket-level breakdown depends on AWS Cost Explorer")
        report.append("configuration and may show aggregated costs.")
        report.append("="*80)
        
        return "\n".join(report)


# Example usage
if __name__ == "__main__":
    # Initialize the cost estimator
    # You can pass AWS credentials here or use environment variables/IAM roles
    estimator = S3CostEstimator()
    
    # Generate cost estimate report with specific region
    report_path = estimator.Cost_Estimate(output_dir="./s3_cost_reports", region="us-west-2")
    
    # Or use default region from instance
    report_path = estimator.Cost_Estimate(output_dir="./s3_cost_reports")
    
    if report_path:
        print(f"Report generated successfully: {report_path}")
    else:
        print("Failed to generate report")
