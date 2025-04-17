import time
import logging
from typing import Optional, Dict, Any
import boto3
from datetime import datetime, timedelta

class MonitoringService:
    """Service for monitoring scraper health, performance, and metrics."""

    def __init__(self, cloudwatch_namespace: str = 'ScraperMetrics'):
        """
        Initialize the monitoring service.
        
        Args:
            cloudwatch_namespace (str): CloudWatch namespace for metrics
        """
        self.cloudwatch = boto3.client('cloudwatch')
        self.namespace = cloudwatch_namespace
        self.logger = logging.getLogger(__name__)

    def record_scrape_duration(self, source: str, duration_seconds: float) -> None:
        """
        Record the duration of a scraping operation.
        
        Args:
            source (str): Source of the scrape (e.g., scraper name)
            duration_seconds (float): Duration in seconds
        """
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[{
                    'MetricName': 'ScrapeDuration',
                    'Value': duration_seconds,
                    'Unit': 'Seconds',
                    'Dimensions': [
                        {'Name': 'Source', 'Value': source}
                    ]
                }]
            )
        except Exception as e:
            self.logger.error(f"Failed to record scrape duration: {e}")

    def record_items_scraped(self, source: str, count: int) -> None:
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
                    'Dimensions': [
                        {'Name': 'Source', 'Value': source}
                    ]
                }]
            )
        except Exception as e:
            self.logger.error(f"Failed to record items scraped: {e}")

    def record_error(self, source: str, error_type: str) -> None:
        """
        Record an error occurrence.
        
        Args:
            source (str): Source of the error
            error_type (str): Type of error
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
                    ]
                }]
            )
        except Exception as e:
            self.logger.error(f"Failed to record error: {e}")

    def check_health(self, source: str, window_minutes: int = 60) -> Dict[str, Any]:
        """
        Check the health of a scraper over a time window.
        
        Args:
            source (str): Source to check
            window_minutes (int): Time window to check in minutes
            
        Returns:
            Dict[str, Any]: Health metrics
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=window_minutes)

            # Get error count
            error_response = self.cloudwatch.get_metric_statistics(
                Namespace=self.namespace,
                MetricName='Errors',
                Dimensions=[{'Name': 'Source', 'Value': source}],
                StartTime=start_time,
                EndTime=end_time,
                Period=window_minutes * 60,
                Statistics=['Sum']
            )

            # Get items scraped
            items_response = self.cloudwatch.get_metric_statistics(
                Namespace=self.namespace,
                MetricName='ItemsScraped',
                Dimensions=[{'Name': 'Source', 'Value': source}],
                StartTime=start_time,
                EndTime=end_time,
                Period=window_minutes * 60,
                Statistics=['Sum', 'Average']
            )

            # Get average duration
            duration_response = self.cloudwatch.get_metric_statistics(
                Namespace=self.namespace,
                MetricName='ScrapeDuration',
                Dimensions=[{'Name': 'Source', 'Value': source}],
                StartTime=start_time,
                EndTime=end_time,
                Period=window_minutes * 60,
                Statistics=['Average']
            )

            return {
                'errors': sum(point['Sum'] for point in error_response['Datapoints']),
                'items_scraped': sum(point['Sum'] for point in items_response['Datapoints']),
                'avg_items_per_scrape': next((point['Average'] for point in items_response['Datapoints']), 0),
                'avg_duration': next((point['Average'] for point in duration_response['Datapoints']), 0),
                'window_minutes': window_minutes,
                'timestamp': end_time.isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to check health: {e}")
            return {
                'error': str(e),
                'window_minutes': window_minutes,
                'timestamp': end_time.isoformat()
            }

    def set_alert(self, source: str, metric_name: str, threshold: float, comparison: str, period_minutes: int = 5) -> None:
        """
        Set up a CloudWatch alarm for a metric.
        
        Args:
            source (str): Source to monitor
            metric_name (str): Metric to monitor
            threshold (float): Threshold value
            comparison (str): Comparison operator ('GreaterThanThreshold', 'LessThanThreshold', etc.)
            period_minutes (int): Evaluation period in minutes
        """
        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName=f"{source}-{metric_name}-Alert",
                MetricName=metric_name,
                Namespace=self.namespace,
                Dimensions=[{'Name': 'Source', 'Value': source}],
                Period=period_minutes * 60,
                EvaluationPeriods=1,
                Threshold=threshold,
                ComparisonOperator=comparison,
                Statistic='Average',
                ActionsEnabled=True
            )
        except Exception as e:
            self.logger.error(f"Failed to set alert: {e}")

    def get_performance_metrics(self, source: str, days: int = 7) -> Dict[str, Any]:
        """
        Get detailed performance metrics for a scraper.
        
        Args:
            source (str): Source to analyze
            days (int): Number of days to analyze
            
        Returns:
            Dict[str, Any]: Performance metrics
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)

            # Get daily success rate
            success_rate = []
            for day in range(days):
                day_start = end_time - timedelta(days=day+1)
                day_end = end_time - timedelta(days=day)
                
                total_runs = self.cloudwatch.get_metric_statistics(
                    Namespace=self.namespace,
                    MetricName='ScrapeDuration',
                    Dimensions=[{'Name': 'Source', 'Value': source}],
                    StartTime=day_start,
                    EndTime=day_end,
                    Period=86400,  # 24 hours
                    Statistics=['SampleCount']
                )

                errors = self.cloudwatch.get_metric_statistics(
                    Namespace=self.namespace,
                    MetricName='Errors',
                    Dimensions=[{'Name': 'Source', 'Value': source}],
                    StartTime=day_start,
                    EndTime=day_end,
                    Period=86400,
                    Statistics=['Sum']
                )

                total = sum(point['SampleCount'] for point in total_runs['Datapoints'])
                error_count = sum(point['Sum'] for point in errors['Datapoints'])
                
                if total > 0:
                    success_rate.append({
                        'date': day_start.date().isoformat(),
                        'success_rate': ((total - error_count) / total) * 100
                    })

            return {
                'source': source,
                'period_days': days,
                'success_rate': success_rate,
                'timestamp': end_time.isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to get performance metrics: {e}")
            return {
                'error': str(e),
                'source': source,
                'period_days': days,
                'timestamp': end_time.isoformat()
            } 