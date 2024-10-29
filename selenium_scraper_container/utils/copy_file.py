"""

This function copies a given file and stores into LTR_storage
"""


import os,sys
import shutil
from simple_chalk import chalk

#get path to src
src_dir = os.path.join(os.getcwd(),'src')
#path to utils dir
LTR_dir = os.path.join(src_dir,"LTR_storage")

test_input_file = os.path.join(src_dir,'file_output','italist_2024-13-09_prada_bags.csv')

def copy_file(input_file_path):

    #extract filename from file_path
    file_name = 'COPY_' + input_file_path.split('file_output/')[1]
    
    #create dest file_path where copied file will be stored
    dest_file_path = os.path.join(LTR_dir,file_name)
    print(dest_file_path)

    #  #create dest dir if doesnt exist 
    # if not os.path.exists(dest_file_path):
    #     os.makedirs(dest_file_path)
    #     print(chalk.green(f"Created directory: {dest_file_path}"))
              
    #create copy
    shutil.copy(input_file_path,dest_file_path)


copy_file(test_input_file)


"""
receive file name, make new file in LTR dir

to make new file in LTR dir, we specify the dest file path
which is ltr/filename
"""