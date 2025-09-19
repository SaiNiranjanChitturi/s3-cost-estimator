#!/usr/bin/env python3
"""
Integrated S3 Analyzer
Combines S3 object listing and cost estimation functionality
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Optional

# Import from our existing modules
from aws_cost_estimator import S3CostEstimator
from display_s3objects import list_s3_objects, list_all_buckets, check_aws_credentials


class IntegratedS3Analyzer:
    """
    Combines S3 object analysis with cost estimation
    """
    
    def __init__(self, region: str = "us-east-1"):
        """
        Initialize the integrated analyzer
        
        Args:
            region: AWS region to use
        """
        self.region = region
        self.cost_estimator = S3CostEstimator(region_name=region)
        
    def analyze_all_buckets(self, output_dir: str = "./s3_analysis") -> Dict:
        """
        Perform comprehensive analysis of all accessible S3 buckets
        
        Args:
            output_dir: Directory to save analysis results
            
        Returns:
            Dictionary containing analysis results
        """
        print("🚀 Starting Comprehensive S3 Analysis")
        print("=" * 50)
        
        # Check credentials first
        if not check_aws_credentials():
            return {"error": "AWS credentials not configured"}
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Get all accessible buckets
        buckets = list_all_buckets()
        if not buckets:
            return {"error": "No accessible buckets found"}
        
        analysis_results = {
            "timestamp": datetime.now().isoformat(),
            "region": self.region,
            "total_buckets": len(buckets),
            "buckets": {}
        }
        
        print(f"\n📊 Analyzing {len(buckets)} buckets...")
        
        for i, bucket_name in enumerate(buckets, 1):
            print(f"\n[{i}/{len(buckets)}] Analyzing bucket: {bucket_name}")
            
            bucket_analysis = self.analyze_single_bucket(bucket_name)
            analysis_results["buckets"][bucket_name] = bucket_analysis
            
            # Show progress
            object_count = bucket_analysis.get("object_count", 0)
            print(f"   ✓ Objects: {object_count}")
        
        # Generate cost report
        print(f"\n💰 Generating cost report...")
        cost_report_path = self.cost_estimator.Cost_Estimate(output_dir=output_dir)
        analysis_results["cost_report_path"] = cost_report_path
        
        # Save combined analysis
        analysis_file = os.path.join(output_dir, f"s3_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        try:
            with open(analysis_file, 'w') as f:
                json.dump(analysis_results, f, indent=2, default=str)
            print(f"📄 Analysis saved to: {analysis_file}")
            analysis_results["analysis_file_path"] = analysis_file
        except Exception as e:
            print(f"❌ Error saving analysis: {e}")
        
        return analysis_results
    
    def analyze_single_bucket(self, bucket_name: str) -> Dict:
        """
        Analyze a single S3 bucket
        
        Args:
            bucket_name: Name of the bucket to analyze
            
        Returns:
            Dictionary with bucket analysis
        """
        analysis = {
            "bucket_name": bucket_name,
            "timestamp": datetime.now().isoformat(),
            "object_count": 0,
            "objects": [],
            "object_types": {},
            "total_size_estimate": "N/A (requires GetObject permission)",
            "folder_structure": {}
        }
        
        try:
            # Get object list
            objects = list_s3_objects(bucket_name, region=self.region)
            analysis["object_count"] = len(objects)
            analysis["objects"] = objects
            
            # Analyze object types and folder structure
            for obj_key in objects:
                # File extension analysis
                if '.' in obj_key:
                    ext = obj_key.split('.')[-1].lower()
                    analysis["object_types"][ext] = analysis["object_types"].get(ext, 0) + 1
                else:
                    analysis["object_types"]["no_extension"] = analysis["object_types"].get("no_extension", 0) + 1
                
                # Folder structure analysis
                if '/' in obj_key:
                    folder = obj_key.split('/')[0]
                    analysis["folder_structure"][folder] = analysis["folder_structure"].get(folder, 0) + 1
                else:
                    analysis["folder_structure"]["root"] = analysis["folder_structure"].get("root", 0) + 1
            
        except Exception as e:
            analysis["error"] = str(e)
        
        return analysis
    
    def generate_summary_report(self, analysis_results: Dict, output_dir: str = "./s3_analysis") -> str:
        """
        Generate a human-readable summary report
        
        Args:
            analysis_results: Results from analyze_all_buckets
            output_dir: Directory to save the report
            
        Returns:
            Path to the generated summary report
        """
        if "error" in analysis_results:
            return ""
        
        report_lines = []
        report_lines.append("🔍 S3 COMPREHENSIVE ANALYSIS SUMMARY")
        report_lines.append("=" * 60)
        report_lines.append(f"Generated: {analysis_results['timestamp']}")
        report_lines.append(f"Region: {analysis_results['region']}")
        report_lines.append(f"Total Buckets: {analysis_results['total_buckets']}")
        report_lines.append("")
        
        # Bucket overview
        report_lines.append("📊 BUCKET OVERVIEW")
        report_lines.append("-" * 30)
        
        total_objects = 0
        all_extensions = {}
        
        for bucket_name, bucket_data in analysis_results["buckets"].items():
            object_count = bucket_data.get("object_count", 0)
            total_objects += object_count
            
            report_lines.append(f"📂 {bucket_name}")
            report_lines.append(f"   Objects: {object_count}")
            
            # Aggregate file types
            for ext, count in bucket_data.get("object_types", {}).items():
                all_extensions[ext] = all_extensions.get(ext, 0) + count
            
            # Show folder structure if exists
            folders = bucket_data.get("folder_structure", {})
            if len(folders) > 1:  # More than just root
                report_lines.append(f"   Folders: {', '.join([f for f in folders.keys() if f != 'root'])}")
            
            report_lines.append("")
        
        # Overall statistics
        report_lines.append("📈 OVERALL STATISTICS")
        report_lines.append("-" * 30)
        report_lines.append(f"Total Objects Across All Buckets: {total_objects}")
        report_lines.append("")
        
        if all_extensions:
            report_lines.append("📄 FILE TYPE DISTRIBUTION")
            report_lines.append("-" * 30)
            sorted_extensions = sorted(all_extensions.items(), key=lambda x: x[1], reverse=True)
            for ext, count in sorted_extensions[:10]:  # Top 10
                percentage = (count / total_objects) * 100 if total_objects > 0 else 0
                report_lines.append(f"   {ext:<15}: {count:>6} files ({percentage:>5.1f}%)")
            report_lines.append("")
        
        # Cost information reference
        if analysis_results.get("cost_report_path"):
            report_lines.append("💰 COST INFORMATION")
            report_lines.append("-" * 25)
            report_lines.append(f"Detailed cost report: {analysis_results['cost_report_path']}")
            report_lines.append("")
        
        report_lines.append("=" * 60)
        report_lines.append("📌 For detailed analysis data, see:")
        report_lines.append(f"   JSON file: {analysis_results.get('analysis_file_path', 'N/A')}")
        report_lines.append("=" * 60)
        
        # Save summary report
        summary_file = os.path.join(output_dir, f"s3_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        try:
            with open(summary_file, 'w') as f:
                f.write("\n".join(report_lines))
            print(f"📋 Summary report saved to: {summary_file}")
            return summary_file
        except Exception as e:
            print(f"❌ Error saving summary: {e}")
            return ""


def main():
    """
    Main function demonstrating the integrated analyzer
    """
    print("🎯 AWS S3 Integrated Analyzer")
    print("Combines object listing and cost analysis")
    print("=" * 50)
    
    # Initialize analyzer
    analyzer = IntegratedS3Analyzer(region="us-east-1")
    
    # Perform comprehensive analysis
    results = analyzer.analyze_all_buckets(output_dir="./s3_comprehensive_analysis")
    
    if "error" not in results:
        # Generate summary report
        summary_path = analyzer.generate_summary_report(results, output_dir="./s3_comprehensive_analysis")
        
        print(f"\n🎉 Analysis Complete!")
        print(f"📊 Analyzed {results['total_buckets']} buckets")
        print(f"📄 Summary: {summary_path}")
        print(f"💰 Cost Report: {results.get('cost_report_path', 'N/A')}")
        print(f"🔍 Detailed Data: {results.get('analysis_file_path', 'N/A')}")
    else:
        print(f"❌ Analysis failed: {results['error']}")


if __name__ == "__main__":
    main()
