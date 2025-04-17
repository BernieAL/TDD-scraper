"""
Script to reorganize project structure
"""
import os
import shutil

def create_structure():
    # # Move documentation files
    # shutil.move('backend/aws/util_scripts/SCRIPTS_README.md', 'backend/docs/')
    # shutil.move('PROJECT_DEV_STAGES.md', 'backend/docs/')
    # shutil.move('backend/OLD_local/misc_commands.txt', 'backend/docs/misc_commands.md')
    
    # # Move test files
    # shutil.move('backend/tests/lambda_functions/test_full_pipeline.py', 
    #             'backend/tests/aws/lambda_functions/')
    

    """
    for each file in workers/
        make a subdir for it
        make __init__.py for each subdir
        move it to subdir
        
    get path to workers dir,
        use os.walk to go through all dir contents
            for each worker,
                use worker name to create subdir for worker
                    create __init__.py file in that subdir
                    then move the current worker into that subdir
    
                    
    """
    worker_dir = os.path.join(os.path.dirname(__file__),'workers')
    
    """
    using listdir() because it lists contents where walk() goes through all subdirs
    for file in files of worker_dir
        if os.path.join(worker_dir,f) isfile, 
        add it to the list

        we omit __init__.py and any non .py files
        this is under the assumption that workers/ only has
        worker .py files
    """
    worker_files = [f for f in os.listdir(worker_dir) 
                    if os.path.isfile(os.path.join(worker_dir,f))
                    and '.py' in f 
                    and f != '__init__.py'
                    ]
    # print(worker_files)


    for worker_file in worker_files:
            # ex scraper_worker.py -> ['scraper_worker','.py']
            worker_name = worker_file.split('.py')[0]
            print(f"Processing: {worker_name}")

            #create full path for new dir
            new_dir_path = os.path.join(worker_dir,worker_name)

            #already inside workers_dir, make dir using worker_file_name
            try:
                #create dir
                os.makedirs(new_dir_path,exist_ok=True)
                print(f"Created directory: {new_dir_path}")

                #create __init__.py
                #build init path, concat with new_dir_path
                init_path = os.path.join(new_dir_path, '__init__.py')
                with open(init_path,'w') as f:
                    pass #make empty file
                print(f"Created: {init_path}")

                #move worker file to new dir
                #build src and dest path to use with shutil.move()
                    
                #src is where file currently is -> workers/
                src = os.path.join(worker_dir,worker_file)

                #dest is where we are going, which is newly created worker_dir path
                #given by new_dir_path and we concat with "worker.py"
                #
                dest = os.path.join(os.path.join(new_dir_path,worker_file),"worker.py")

                shutil.move(src,dest)


                print(f"Moved {src} -> {dest}")

            except Exception as e:
                print(f"Error processing {worker_name}: {e}")

    # # Create __init__.py files
    # init_paths = [
    #     'backend/aws/db',
    #     'backend/aws/s3',
    #     'backend/aws/lambda_functions',
    #     'backend/workers',
    #     'backend/workers/compare_worker',
    #     'backend/workers/price_worker',
    #     'backend/config',
    #     'backend/tests/aws/db',
    #     'backend/tests/aws/s3',
    #     'backend/tests/aws/lambda_functions',
    #     'backend/tests/workers/compare_worker',
    #     'backend/tests/workers/price_worker'
    # ]
    
    # for path in init_paths:
    #     with open(f'{path}/__init__.py', 'w') as f:
    #         pass

    # # Remove old directories
    # shutil.rmtree('backend/OLD_local')
    # shutil.rmtree('backend/aws/util_scripts')

    # # Update .PROJECT_ROOT
    # with open('backend/.PROJECT_ROOT', 'w') as f:
    #     f.write('Project root marker file\n') 
    
    #  # Move worker files to their new locations
    # worker_moves = {
    #     'backend/workers/compare_worker.py': 'backend/workers/compare_worker/worker.py',
    #     'backend/workers/scraper_worker.py': 'backend/workers/scraper_worker/worker.py',
    #     'backend/workers/search_request_worker.py': 'backend/workers/search_request_worker/worker.py',
    #     'backend/workers/email_sender.py': 'backend/workers/email_sender/sender.py',
    #     'backend/analysis/compare_data.py': 'backend/workers/compare_worker/compare_data.py'
    # }

    # # for each src,dest pair in worker_moves
    # for src,dest in worker_moves.items():
    #     #make the dir if it doesnt exist, if exists do nothing
    #     os.makedirs(os.path.dirname(dest),exist_ok=True)
    #     #moves file from src to dest path (cut and paste operation)
    #     shutil.move(src, dest)

if __name__ == '__main__':
    create_structure()