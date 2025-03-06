"""
Script to reorganize project structure
"""
import os
import shutil

def create_structure():
    # Move documentation files
    shutil.move('backend/aws/util_scripts/SCRIPTS_README.md', 'backend/docs/')
    shutil.move('PROJECT_DEV_STAGES.md', 'backend/docs/')
    shutil.move('backend/OLD_local/misc_commands.txt', 'backend/docs/misc_commands.md')
    
    # Move test files
    shutil.move('backend/tests/lambda_functions/test_full_pipeline.py', 
                'backend/tests/aws/lambda_functions/')
    
    # Create __init__.py files
    init_paths = [
        'backend/aws/db',
        'backend/aws/s3',
        'backend/aws/lambda_functions',
        'backend/workers',
        'backend/workers/compare_worker',
        'backend/workers/price_worker',
        'backend/config',
        'backend/tests/aws/db',
        'backend/tests/aws/s3',
        'backend/tests/aws/lambda_functions',
        'backend/tests/workers/compare_worker',
        'backend/tests/workers/price_worker'
    ]
    
    for path in init_paths:
        with open(f'{path}/__init__.py', 'w') as f:
            pass

    # Remove old directories
    shutil.rmtree('backend/OLD_local')
    shutil.rmtree('backend/aws/util_scripts')

    # Update .PROJECT_ROOT
    with open('backend/.PROJECT_ROOT', 'w') as f:
        f.write('Project root marker file\n') 