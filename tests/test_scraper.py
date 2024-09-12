import pytest, requests, time
from src.scraper import fetch_html,parse_html,save_report
from unittest.mock import MagicMock,patch
from selenium import webdriver
from chromedriver_py import binary_path
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

from src.italist_scrape_test import italist_scrape

@patch("requests.get")
def test_mocked_request(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "<html><title>Mocked Title</title></html>"
    
    result = fetch_html("https://example.com")
    assert "<title>Mocked Title</title>" in result


def test_missing_element():
    html = "<html><head></head><body>No title here</body></html>"
    result = parse_html(html)
    assert result is None  # Assuming your function returns None if title is missing

def test_broken_html():
    html = "<html><head><title>Broken Title<body>Content without closing tags"
    result = parse_html(html)
    assert result == "Broken Title"

def test_nested_structure():
    html = "<html><body><div><p><a href='https://example.com'>Nested Link</a></p></div></body></html>"
    result = scrape_links(html)
    assert result == ["https://example.com"]

"""
test that fetch_html can retrieve html content
"""
def test_fetch_html():
    url = "https://example.com"
    html = fetch_html(url)
    assert "<title>Example Domain</title>" in html

"""
test that parse_html function can parse given html
"""
def test_parse_html():
    html = "<html><head><title>Test Title</title></head></html>"
    title = parse_html(html)
    assert title == "Test Title"

"""
test that function can generate a report from data
"""
def test_save_report():
    data = {"title": "Test Title"}
    save_report(data)
    with open('report.json', 'r') as f:
        content = f.read()
    assert content == '{"title": "Test Title"}'


def test_data_cleanup():
    html = "<html><body>Some data &nbsp; with spaces</body></html>"
    cleaned_data = clean_html(html)
    assert cleaned_data == "Some data with spaces"

def test_duplicate_removal():
    links = ["https://example.com", "https://example.com", "https://example.org"]
    unique_links = remove_duplicates(links)
    assert unique_links == ["https://example.com", "https://example.org"]


def test_pagination():
    urls = ["https://example.com/page1", "https://example.com/page2"]
    expected_titles = ["Title Page 1", "Title Page 2"]
    
    results = []
    for url in urls:
        html = fetch_html(url)
        title = parse_html(html)
        results.append(title)
    
    assert results == expected_titles


def test_csv_storage():
    data = [{"col1": "value1", "col2": "value2"}]
    save_to_csv(data, "output.csv")
    
    with open("output.csv", "r") as f:
        content = f.read()
    
    assert "value1" in content
    assert "value2" in content

def test_report_generation():
    data = {"title": "Test Title", "links": ["https://example.com"]}
    save_report(data, "report.txt")
    
    with open("report.txt", "r") as f:
        content = f.read()
    
    assert "Test Title" in content
    assert "https://example.com" in content


def test_rate_limiting():
    urls = ["https://example.com/page1", "https://example.com/page2"]
    start_time = time.time()
    
    for url in urls:
        fetch_html(url)
        time.sleep(2)  # Simulating rate limit
    
    end_time = time.time()
    assert end_time - start_time >= 4  # Should take at least 4 seconds for 2 requests

def test_retry_logic(requests_mock):
    url = "https://example.com"
    requests_mock.get(url, [dict(status_code=500), dict(status_code=200, text="Success")])
    
    result = fetch_html_with_retry(url, retries=2)
    assert result == "Success"

def test_javascript_rendering():
    svc = webdriver.ChromeService(executable_path=binary_path)
    driver = webdriver.Chrome(service=svc)
    
    driver.get("https://example.com")
    content = driver.page_source
    
    assert "Expected Content" in content
    driver.quit()



@patch('src.italist_scrape_test.get_driver')
def test_italist_extraction(mock_get_driver):

    """
    patch the get_driver function - this mocks browser driver to avoid using real browser

    mock find_element behavior:
        for each listing, find_element calls are mocked to return MagicMock elements 
        that simulate text of the web elements (brand,product name,price)

    side_effect is used to simulate different cases
        for mock_listing_1 - all elements are found successfully

        for mock_listing_2 the sales_price is missing, but regular price is found

    
    """

    #create mock web driver and mock elements
    mock_driver = MagicMock()
    mock_get_driver.return_value = mock_driver

    #create mock for product grid container
    mock_product_container = MagicMock()

    #create mock listings
    mock_listing_1 = MagicMock()
    mock_listing_2 = MagicMock()

    #mock first listing with all elements (brand,productName,sales-price)
    mock_listing_1.find_element.side_effect = [
        MagicMock(text='Prada'),
        MagicMock(text='Black Re-nylon Beauty Case'),
        MagicMock(text='USD 418')
    ]

     # Mock the second listing with no sales-price, only regular price
    mock_listing_2.find_element.side_effect = [
        MagicMock(text='Gucci'),                # brand
        MagicMock(text='Leather Belt'),         # product name
        NoSuchElementException,                 # sales-price not found
        MagicMock(text='USD 500')               # regular price
    ]
    
    #mock product grid contaner and its listings
    mock_driver.find_element.return_value = mock_product_container
    mock_product_container.find_elements.return_value = [mock_listing_1,mock_listing_2]

    #call function we want to test
    italist_scrape()

    #assrt driver.get was called with correct file path
    mock_driver.get.assert_called_once()

    #assert mock find_element was called correctly for each listing
    assert mock_listing_1.find_element.call_count == 3 # brand, productName, sales-price
    assert mock_listing_2.find_element.call_count == 4 # brand, productName, sales-price (fail), price
    