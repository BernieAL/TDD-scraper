from setuptools import setup, find_packages

setup(
    name="tdd-scraper-shared",
    version="0.1.0",
    packages=find_packages(where="shared"),
    package_dir={"": "shared"},
    install_requires=[
        "boto3>=1.26.137",
        "requests>=2.28.2",
        "python-dotenv>=1.0.0",
    ],
    python_requires=">=3.8",
    description="Shared utilities for TDD Scraper project",
    author="TDD Scraper Team"
) 