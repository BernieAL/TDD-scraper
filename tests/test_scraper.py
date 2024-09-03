import pytest 
from src.scraper import fetch_html,parse_html,save_report


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
