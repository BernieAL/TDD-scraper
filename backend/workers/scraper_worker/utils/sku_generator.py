from pathlib import Path
import csv
import pandas as pd
from typing import Dict
import hashlib


def generate_master_sku(product_name: str) -> str:
    """
    Generate master SKU using hash of product name
    Example: "Louis Vuitton Neverfull MM" -> "LV-B-NV-a7b2c"

    opted for hash becuase its deterministic as opposed to prev idea which was 
    added random numbers to end of sku
    """
    tokens = product_name.split()
    
    # Brand initials (LV)
    brand = ''.join(word[0].upper() for word in tokens[:2])
    
    # # Product type (B)
    # product_type = 'B'
    
    # Model initials (NV)
    model = tokens[2][:2].upper()
    
    # Generate hash from full product name
    hash_object = hashlib.md5(product_name.encode())
    identifier = hash_object.hexdigest()[:5]  # Take first 5 chars of hash
    
    # return f"{brand}-{product_type}-{model}-{identifier}"
    return f"{brand}-{model}-{identifier}"


def generate_master_sku_col(raw_file_path: Path) -> None:
    """
    Read in raw CSV
    find unique products
    generate skus
    add skus back to data as first col
    save updated csv back in original dir position
    """

    try:
        df = pd.read_csv(raw_file_path)

        #get unique product names
        unique_products = df['product_name'].unique()

        #genrate sku mapping for each product name
        #this creates dictionary mapping product names to skus
        """
            sku_mapping now contains:
            {
                'LV Neverfull': 'LV-B-NV-421k02',
                'LV Speedy': 'LV-B-SP-892j01'
            }
        """
        sku_mapping: Dict[str,str] = {}
        for product in unique_products:
            sku_mapping[product] = generate_master_sku(product)

        #for each val in product_name col
        #look up that val in sku_mapping dict, retrieves sku for this val
        #maps sku to product name
        #it does this for all vals in product_name. the mapped results are stored in a master_sku col using the mapping
        #we are matching product name to sku, then storing this as a new col master_sku, where 
        #Ex. # 1. Takes first value 'LV Neverfull' -> looks up in sku_mapping -> gets 'LV-B-NV-421k02'
        df['master_sku'] = df['product_name'].map(sku_mapping)
        """
        Final DataFrame:
        product_name,    price,  source,     master_sku
        LV Neverfull,   1000,   italist,    LV-B-NV-421k02
        LV Speedy,      2000,   farfetch,   LV-B-SP-892j01
        LV Neverfull,   1100,   mytheresa,  LV-B-NV-421k02  # Same SKU as first row
        """


    except Exception as e:
        print(f"Error processing {raw_file_path}:{e}")


def process_scraped_file(file_path: str) -> dict:
    """
    Process scraped file to generate SKUs and organize data.

    Args:
        file_path (str): Path to scraped data file

    Returns:
        dict: Processed data with SKUs
    
    Example:
        >>> process_scraped_file('/temp/italist/RAW_ITALIST_PRADA_DATE_BAGS.csv')
        {
            'processed_data': [...],
            'skus_generated': [...],
            'stats': {...}
        }
    """
    try:
        # For now, return mock data for testing
        return {
            'processed_data': [],
            'skus_generated': [],
            'stats': {
                'total_products': 0,
                'skus_generated': 0
            }
        }
    except Exception as e:
        raise Exception(f"Error processing scraped file: {str(e)}")