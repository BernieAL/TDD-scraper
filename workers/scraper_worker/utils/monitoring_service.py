import boto3
import logging
from datetime import datetime

class MonitoringService:
    """Service for monitoring scraper metrics using CloudWatch."""

    def __init__(self, cloudwatch_namespace: str = 'ScraperMetrics'):
        """
        Initialize the monitoring service.
        
        Args:
            cloudwatch_namespace (str): CloudWatch namespace for metrics
        """
        self.cloudwatch = boto3.client('cloudwatch', endpoint_url='http://localhost:4566')
        self.namespace = cloudwatch_namespace
        self.logger = logging.getLogger(__name__)

    def record_scrape_duration(self, source: str, duration: float):
        """
        Record the duration of a scraping operation.
        
        Args:
            source (str): Source of the scrape (e.g., 'VESTIAIRE', 'REBAG')
            duration (float): Duration in seconds
        """
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[{
                    'MetricName': 'ScrapeDuration',
                    'Value': duration,
                    'Unit': 'Seconds',
                    'Dimensions': [{'Name': 'Source', 'Value': source}],
                    'Timestamp': datetime.now()
                }]
            )
        except Exception as e:
            self.logger.error(f"Failed to record scrape duration: {str(e)}")

    def record_items_scraped(self, source: str, count: int):
        """
        Record the number of items scraped.
        
        Args:
            source (str): Source of the scrape
            count (int): Number of items scraped
        """
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[{
                    'MetricName': 'ItemsScraped',
                    'Value': count,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Source', 'Value': source}],
                    'Timestamp': datetime.now()
                }]
            )
        except Exception as e:
            self.logger.error(f"Failed to record items scraped: {str(e)}")

    def record_error(self, source: str, error_type: str):
        """
        Record an error occurrence.
        
        Args:
            source (str): Source where the error occurred
            error_type (str): Type of error (e.g., 'ExtractListingError', 'EmptyDataError')
        """
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[{
                    'MetricName': 'Errors',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'Source', 'Value': source},
                        {'Name': 'ErrorType', 'Value': error_type}
                    ],
                    'Timestamp': datetime.now()
                }]
            )
        except Exception as e:
            self.logger.error(f"Failed to record error: {str(e)}")

    def create_alarm(self, source: str, metric_name: str, threshold: float, period: int = 300):
        """
        Create a CloudWatch alarm for a metric.
        
        Args:
            source (str): Source to monitor
            metric_name (str): Name of the metric to monitor
            threshold (float): Threshold value for the alarm
            period (int): Period in seconds over which to evaluate the alarm
        """
        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName=f"{source}-{metric_name}-Alert",
                MetricName=metric_name,
                Namespace=self.namespace,
                Period=period,
                EvaluationPeriods=1,
                Threshold=threshold,
                ComparisonOperator='GreaterThanThreshold',
                Statistic='Sum',
                Dimensions=[{'Name': 'Source', 'Value': source}],
                AlarmDescription=f'Alert for {metric_name} in {source}',
                AlarmActions=[]  # Add SNS topic ARN here if needed
            )
        except Exception as e:
            self.logger.error(f"Failed to create alarm: {str(e)}")

    def get_metric_statistics(self, source: str, metric_name: str, start_time: datetime, end_time: datetime):
        """
        Get statistics for a metric over a time period.
        
        Args:
            source (str): Source to get metrics for
            metric_name (str): Name of the metric
            start_time (datetime): Start time for the query
            end_time (datetime): End time for the query
            
        Returns:
            dict: Metric statistics
        """
        try:
            return self.cloudwatch.get_metric_statistics(
                Namespace=self.namespace,
                MetricName=metric_name,
                Dimensions=[{'Name': 'Source', 'Value': source}],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=['Sum', 'Average', 'Maximum']
            )
        except Exception as e:
            self.logger.error(f"Failed to get metric statistics: {str(e)}")
            return None 