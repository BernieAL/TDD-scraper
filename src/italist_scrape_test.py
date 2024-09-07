import requests,os
from bs4 import BeautifulSoup
import pytest
from pathlib import Path

# Create a session object
session = requests.Session()

#target purse
target = "locky bb"

urls = [
    # 'https://www.italist.com/us/',
    'https://www.cettire.com/pages/search?q=locky%20bb&qTitle=locky%20bb&menu%5Bdepartment%5D=women',
    'https://shop.rebag.com/search?q=locky+bb',
    'https://us.vestiairecollective.com/search/?q=locky+bb#gender=Women%231',
    'https://www.fashionphile.com/shop?search=locky+bb',
    # 'https://www.therealreal.com/' #requires login
]

#for each url in urls, nav to and search for target purse
#the process of this could be different for each depending on site layout. 
# Define a function to fetch the page content
def fetch_page(url):
    response = requests.get(url)
    response.raise_for_status()  # Ensure we get a valid response
    return response.text


#extract html
def parse_url1(html):
    soup = BeautifulSoup(html, 'html.parser')
    
# def scrape_url(url):
#     html = fetch_page(url)
#     if "example1.com" in url:
#         return parse_url1(html)
#     elif "example2.com" in url:
#         return parse_url2(html)
#     else:
#         return {'error': 'No parser defined for this URL'}

#parse url name to use as filename
def parse_url(url):

    url_tokens = url.split("//")
    url_tokens = url_tokens[1].split(".com")
    name = url_tokens = url_tokens[0]
    return name

"""
Creates file in file_output dir using name
"""
def create_file(name):
    #get cwd - src dir
    src_dir = os.getcwd()

    #build path to src/file_output dir
    file_output_dir = os.path.join(src_dir,"file_output")

    #if dir doesnt exist, make it
    if not os.path.exists(file_output_dir):
        os.makedirs(file_output_dir)
    # print(file_output_dir)
    
    #build file path to create 
    file_to_create = os.path.join(file_output_dir,name)
    print(file_to_create)

    if not os.path.exists(file_to_create):
        #open file and write to it
        with open(file_to_create,'w') as file:
            file.write(" ")

    return file_to_create

def extract_html_url(url):

    response = requests.get(url)
    html = response.text
    return html

def write_file(file_path,html):

    #open file by name, write html content to it
    # file_path = f'{name}.txt'
    file_path = file_path
    with open(file_path,'w',encoding='utf-8') as file:
        file.write(html)

def process_url_runner(urls):
    for url in urls:
        
        name = parse_url(url)
        file_path = create_file(name)
        html = extract_html_url(url)
        write_file(file_path,html)
    

process_url_runner(urls)

    #for given url, make request,



# -----------------------------------------------------------------------------------------------------
def test_parse_url():

    url = "https://us.vestiairecollective.com/"
    url_tokens = url.split("//")
    url_tokens = url_tokens[1].split(".com")
    name = url_tokens = url_tokens[0]
    print(name)
    assert name == 'us.vestiairecollective'


@pytest.fixture
#fixture to handle file creation
def create_file():
    #get cwd - src dir
    src_dir = os.getcwd()

    #build path to src/file_output dir
    file_output_dir = os.path.join(src_dir,"file_output")

    #if dir doesnt exist, make it
    if not os.path.exists(file_output_dir):
        os.makedirs(file_output_dir)
    # print(file_output_dir)
    
    #build file path to create 
    file_to_create = os.path.join(file_output_dir,'us.vestiairecollective.txt')
    print(file_to_create)

    #open file and write to it
    with open(file_to_create,'w') as file:
        file.write("test")

    return file_to_create

# Test to check if the file was created
def test_file_creation(create_file):
    assert os.path.exists(create_file),f"File '{create_file}' was not created."
   
def test_file_content(create_file):
    with open(create_file,'r') as file:
        contents = file.read().strip()

    assert contents == "test",f"File content is '{contents}, but expected 'test'."
