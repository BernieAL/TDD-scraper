"""
SNS Topics Configuration

This module defines all SNS topics used in the price comparison application.
Topics are defined as constants for easy reference and maintenance.

Topics:
    PRICE_CHANGE_TOPIC: For price change notifications
    SOLD_ITEMS_TOPIC: For sold items notifications
    ANALYSIS_COMPLETE_TOPIC: For analysis completion notifications
"""

from dataclasses import dataclass
from typing import Dict

@dataclass
class SNSTopic:
    """Define SNS topic structure"""
    name: str
    display_name: str
    description: str

# Define SNS topics
SNS_TOPICS = {
    'price_change': SNSTopic(
        name='price-change-topic',
        display_name='Price Change Notifications',
        description='Topic for price change notifications'
    ),
    'sold_items': SNSTopic(
        name='sold-items-topic',
        display_name='Sold Items Notifications',
        description='Topic for sold items notifications'
    ),
    'analysis_complete': SNSTopic(
        name='analysis-complete-topic',
        display_name='Analysis Complete Notifications',
        description='Topic for analysis completion notifications'
    )
}

# Topic ARNs (will be populated during setup)
TOPIC_ARNS: Dict[str, str] = {} 