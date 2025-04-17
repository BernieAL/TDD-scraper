"""
Report generator for price analysis results.
"""
import json
from datetime import datetime
from typing import Dict, Any, List

class ReportGenerator:
    """Generates reports from price analysis results."""
    
    def __init__(self, analysis_results: Dict[str, Any]):
        """Initialize the report generator with analysis results.
        
        Args:
            analysis_results: Dictionary containing price analysis results
        """
        self.analysis_results = analysis_results
        self.timestamp = datetime.now().isoformat()
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate a report from the analysis results.
        
        Returns:
            Dictionary containing the report data
        """
        return {
            "timestamp": self.timestamp,
            "query_hash": self.analysis_results.get("query_hash", ""),
            "summary": self._generate_summary(),
            "changes": self.analysis_results.get("changes", []),
            "metadata": self.analysis_results.get("metadata", {})
        }
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate a summary of the analysis results.
        
        Returns:
            Dictionary containing summary information
        """
        changes = self.analysis_results.get("changes", [])
        return {
            "total_items": len(changes),
            "items_with_changes": sum(1 for change in changes if change.get("price_change", 0) != 0),
            "average_price_change": sum(change.get("price_change", 0) for change in changes) / len(changes) if changes else 0,
            "average_percentage_change": sum(change.get("percentage_change", 0) for change in changes) / len(changes) if changes else 0
        }
    
    def save_report(self, bucket_name: str, s3_client) -> str:
        """Save the report to S3.
        
        Args:
            bucket_name: Name of the S3 bucket
            s3_client: Boto3 S3 client
            
        Returns:
            Path to the saved report
        """
        report = self.generate_report()
        report_path = f"reports/{report['query_hash']}/{self.timestamp}.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=report_path,
            Body=json.dumps(report, indent=2)
        )
        
        return report_path 