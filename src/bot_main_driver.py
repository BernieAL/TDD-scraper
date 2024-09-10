import os,sys
from simple_chalk import chalk


#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(chalk.red(parent_dir))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
    sys.path.append(os.path.join(parent_dir,"DB_logic"))

from rbmq.scrape_producer import add_to_queue    

add_to_queue("bot.sannysoft.com")