import json
import logging
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self):
        pass
        
    def generate_report(self, analysis_results: Dict, email: str, query_hash: str) -> Dict:
        """
        Generate a report from analysis results
        """
        try:
            logger.info(f"Generating report for query hash: {query_hash}")
            
            # Create report structure
            report = {
                'query_hash': query_hash,
                'email': email,
                'timestamp': datetime.utcnow().isoformat(),
                'summary': {
                    'total_products': analysis_results['total_products'],
                    'files_analyzed': analysis_results['files_analyzed'],
                    'price_range': {
                        'min': analysis_results['price_stats']['min'],
                        'max': analysis_results['price_stats']['max'],
                        'average': analysis_results['price_stats']['avg']
                    }
                },
                'brand_distribution': analysis_results['products_by_brand'],
                'category_distribution': analysis_results['products_by_category'],
                'recommendations': self._generate_recommendations(analysis_results)
            }
            
            logger.info(f"Report generated: {json.dumps(report)}")
            return report
            
        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}", exc_info=True)
            raise
    
    def _generate_recommendations(self, analysis_results: Dict) -> List[str]:
        """Generate recommendations based on analysis results"""
        recommendations = []
        
        # Price-based recommendations
        if analysis_results['price_stats']['min'] < analysis_results['price_stats']['avg'] * 0.8:
            recommendations.append("Some products are significantly below average price - potential good deals")
        if analysis_results['price_stats']['max'] > analysis_results['price_stats']['avg'] * 1.2:
            recommendations.append("Some products are significantly above average price - consider waiting for price drops")
        
        # Brand distribution recommendations
        if len(analysis_results['products_by_brand']) > 1:
            recommendations.append("Multiple brands available - consider comparing options")
        
        # Category distribution recommendations
        if len(analysis_results['products_by_category']) > 1:
            recommendations.append("Products available in multiple categories - consider exploring different categories")
        
        return recommendations 