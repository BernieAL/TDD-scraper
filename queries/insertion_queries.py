import sys,os

#get parent dir  from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.abspath(__file__))
print(parent_dir)

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)



PRODUCT_INSERT_QUERY = """
    INSERT INTO products(product_id,
                         brand,
                         product_name,
                         curr_price,
                         curr_scrape_date,
                         prev_price,
                         prev_scrape_date,
                         sold_date,
                         sold)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""