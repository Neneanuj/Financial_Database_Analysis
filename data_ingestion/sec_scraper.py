import requests
from bs4 import BeautifulSoup

url = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

dataset_links = [link['href'] for link in soup.find_all('a', href=True) if 'financial-statement' in link['href']]
print(dataset_links)
