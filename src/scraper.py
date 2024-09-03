import requests,json
from bs4 import BeautifulSoup

def fetch_html(url):
    response = requests.get(url)
    response.raise_for_status() #ensure we notice bad response
    return response.text

def parse_html(html):
    soup = BeautifulSoup(html,'html.parser')
    title = soup.title.string
    return title

def save_report(data,filename="report.json"):
    with open(filename,'w') as f:
        json.dump(data,f)