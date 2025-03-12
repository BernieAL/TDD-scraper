from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime

@dataclass
class TableSchema:
    """Define table structure"""
    name: str
    partition_key: str
    sort_key: str
    attributes: Dict[str, str]  # name: type
    indexes: List[Dict] = None  # GSIs/LSIs

# Define table schemas
TABLE_SCHEMAS = {
    'users': TableSchema(
        name='users-table',
        partition_key='PK',      # USER#{user_id}
        sort_key='SK',          # META
        attributes={
            'PK': 'S',
            'SK': 'S',
            'email': 'S',
            'created_at': 'S',
            'last_login': 'S',
            'member_tier': 'N'  #0=free, 1 = lvl 1 paid, 2 = lvl 2 paid
        }
    ),
    'user_searches': TableSchema(
        name='user-searches-table',
        partition_key='PK',      # USER#{user_id}
        sort_key='SK',          # SEARCH#{timestamp}
        attributes={
            'PK': 'S',
            'SK': 'S',
            'query_hash': 'S',
            'brand': 'S',
            'category': 'S',
            'specific_item': 'S',
            'search_date': 'S', #date search was performed
            'timestamp': 'S',
            'status': 'S'  # Track search status
        }
    ),
    'products': TableSchema(
        name='products-table',
        partition_key='PK',      # PROD#{product_id}
        sort_key='SK',          # SOURCE#{source}
        attributes={
            'PK': 'S',
            'SK': 'S',
            'master_sku': 'S',
            'product_name': 'S',
            'current_price': 'N',
            'previous_price': 'N',
            'last_scrape_date': 'S',
            'url': 'S',
            'source': 'S'
        }
    ),
    'price_history': TableSchema(
        name='price-history-table',
        partition_key='PK',      # PROD#{product_id}
        sort_key='SK',          # PRICE#{timestamp}
        attributes={
            'PK': 'S',
            'SK': 'S',
            'master_sku': 'S',
            'high_price': 'N',
            'low_low': 'N',
            'source': 'S',
            'scrape_date': 'S'
        }
    )
}
