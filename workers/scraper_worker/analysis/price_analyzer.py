import logging
from datetime import datetime
from typing import Dict, List, Optional
import boto3
from decimal import Decimal

class PriceAnalyzer:
    """Analyzes price changes and trends for scraped products."""

    def __init__(self, dynamodb_table: str, sns_topic_arn: str):
        """Initialize the price analyzer with DynamoDB table and SNS topic."""
        self.dynamodb = boto3.resource('dynamodb')
        self.sns = boto3.client('sns')
        self.table = self.dynamodb.Table(dynamodb_table)
        self.sns_topic_arn = sns_topic_arn
        self.logger = logging.getLogger(__name__)

    def analyze_price_changes(self, new_data: List[Dict]) -> List[Dict]:
        """
        Analyze price changes by comparing new data with historical prices.
        
        Args:
            new_data: List of newly scraped product data
            
        Returns:
            List of products with significant price changes
        """
        price_changes = []
        errors = []
        
        for product in new_data:
            try:
                # Get historical data from DynamoDB
                historical_data = self._get_historical_data(product)
                
                if historical_data:
                    # Calculate price change
                    current_price = self._parse_price(product['price'])
                    previous_price = self._parse_price(historical_data['price'])
                    
                    if current_price != previous_price:
                        price_change = {
                            'product_id': product.get('id'),
                            'brand': product['brand'],
                            'name': product['name'],
                            'current_price': current_price,
                            'previous_price': previous_price,
                            'price_difference': current_price - previous_price,
                            'percentage_change': ((current_price - previous_price) / previous_price) * 100,
                            'source': product['source'],
                            'url': product['url'],
                            'timestamp': datetime.now().isoformat()
                        }
                        
                        # Store the new price
                        self._update_price_history(product, current_price)
                        
                        price_changes.append(price_change)
                else:
                    # New product, store initial price
                    self._store_initial_price(product)
            
            except Exception as e:
                self.logger.error(f"Error analyzing price for product {product.get('name')}: {str(e)}")
                errors.append(e)
                continue
        
        if errors:
            raise errors[0]  # Raise the first error encountered
        
        return price_changes

    def _get_historical_data(self, product: Dict) -> Optional[Dict]:
        """Get historical price data for a product from DynamoDB."""
        try:
            response = self.table.get_item(
                Key={
                    'product_id': product.get('id'),
                    'source': product['source']
                }
            )
            return response.get('Item')
        except Exception as e:
            self.logger.error(f"Error getting historical data: {str(e)}")
            raise

    def _parse_price(self, price_str: str) -> Decimal:
        """Parse price string to Decimal, handling different formats."""
        try:
            # Remove currency symbols and whitespace
            clean_price = ''.join(c for c in price_str if c.isdigit() or c in '.,')
            return Decimal(clean_price)
        except Exception as e:
            self.logger.error(f"Error parsing price {price_str}: {str(e)}")
            raise

    def _update_price_history(self, product: Dict, new_price: Decimal) -> None:
        """Update price history in DynamoDB."""
        try:
            self.table.update_item(
                Key={
                    'product_id': product.get('id'),
                    'source': product['source']
                },
                UpdateExpression="SET price = :price, last_updated = :timestamp",
                ExpressionAttributeValues={
                    ':price': str(new_price),
                    ':timestamp': datetime.now().isoformat()
                }
            )
        except Exception as e:
            self.logger.error(f"Error updating price history: {str(e)}")
            raise

    def _store_initial_price(self, product: Dict) -> None:
        """Store initial price data for a new product."""
        try:
            self.table.put_item(
                Item={
                    'product_id': product.get('id'),
                    'source': product['source'],
                    'brand': product['brand'],
                    'name': product['name'],
                    'price': product['price'],
                    'url': product['url'],
                    'first_seen': datetime.now().isoformat(),
                    'last_updated': datetime.now().isoformat()
                }
            )
        except Exception as e:
            self.logger.error(f"Error storing initial price: {str(e)}")
            raise

    def generate_price_report(self, price_changes: List[Dict]) -> str:
        """Generate a formatted report of price changes."""
        if not price_changes:
            return "No significant price changes detected."

        report = ["Price Change Report", "=" * 20, ""]
        
        for change in price_changes:
            report.extend([
                f"Product: {change['brand']} - {change['name']}",
                f"Source: {change['source']}",
                f"Previous Price: ${change['previous_price']:.2f}",
                f"Current Price: ${change['current_price']:.2f}",
                f"Change: ${change['price_difference']:.2f} ({change['percentage_change']:.1f}%)",
                f"URL: {change['url']}",
                "-" * 20,
                ""
            ])

        return "\n".join(report)

    def send_report(self, report: str) -> None:
        """Send the price change report via SNS."""
        try:
            self.sns.publish(
                TopicArn=self.sns_topic_arn,
                Subject="Price Change Alert",
                Message=report
            )
            self.logger.info("Price change report sent successfully")
        except Exception as e:
            self.logger.error(f"Error sending report: {str(e)}")
            raise 