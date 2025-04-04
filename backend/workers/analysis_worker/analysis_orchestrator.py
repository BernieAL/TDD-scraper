import json
import logging
import boto3
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

class AnalysisOrchestrator:
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.bucket = 'scraper-data-bucket'
        
    def run_analysis(self, raw_path: str, filtered_path: str, analysis_path: str, query_hash: str) -> Dict:
        """
        Run analysis on scraped data
        """
        try:
            logger.info(f"Starting analysis for query hash: {query_hash}")
            
            # Get all raw files
            raw_files = self._list_s3_files(raw_path)
            logger.info(f"Found {len(raw_files)} raw files to analyze")
            
            # Process each file
            analysis_results = {
                'query_hash': query_hash,
                'timestamp': datetime.utcnow().isoformat(),
                'files_analyzed': len(raw_files),
                'total_products': 0,
                'price_stats': {
                    'min': float('inf'),
                    'max': 0,
                    'avg': 0
                },
                'products_by_brand': {},
                'products_by_category': {}
            }
            
            total_price = 0
            for file_key in raw_files:
                file_data = self._get_s3_file(file_key)
                products = self._parse_csv(file_data)
                
                for product in products:
                    # Update price statistics
                    price = float(product.get('price', 0))
                    analysis_results['price_stats']['min'] = min(analysis_results['price_stats']['min'], price)
                    analysis_results['price_stats']['max'] = max(analysis_results['price_stats']['max'], price)
                    total_price += price
                    
                    # Update brand and category counts
                    brand = product.get('brand', 'Unknown')
                    category = product.get('category', 'Unknown')
                    
                    analysis_results['products_by_brand'][brand] = analysis_results['products_by_brand'].get(brand, 0) + 1
                    analysis_results['products_by_category'][category] = analysis_results['products_by_category'].get(category, 0) + 1
                    
                    analysis_results['total_products'] += 1
            
            # Calculate average price
            if analysis_results['total_products'] > 0:
                analysis_results['price_stats']['avg'] = total_price / analysis_results['total_products']
            
            logger.info(f"Analysis completed: {json.dumps(analysis_results)}")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}", exc_info=True)
            raise
    
    def _list_s3_files(self, prefix: str) -> List[str]:
        """List all files in S3 with given prefix"""
        response = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix
        )
        return [obj['Key'] for obj in response.get('Contents', [])]
    
    def _get_s3_file(self, key: str) -> str:
        """Get file content from S3"""
        response = self.s3.get_object(
            Bucket=self.bucket,
            Key=key
        )
        return response['Body'].read().decode('utf-8')
    
    def _parse_csv(self, csv_data: str) -> List[Dict]:
        """Parse CSV data into list of dictionaries"""
        import csv
        from io import StringIO
        
        products = []
        reader = csv.DictReader(StringIO(csv_data))
        for row in reader:
            products.append(row)
        return products 