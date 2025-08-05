#example web scraper methods in python
import requests
from bs4 import BeautifulSoup

response = requests.get('https://www.gracenote.com')

print(response.status_code)

#Example: Parse HTML using BeautifulSoup
soup = BeautifulSoup(response.content, 'html.parser')

# Example: Extract paragraph content by class and tag
# Find the main content container
content_div = soup.find('div', class_='gracenote-testimonials__item')
if content_div:
    for para in content_div.find_all('p'):
        print(para.text.strip())
else:
    print("No article content found.")


#print(soup.prettify())

#Example 1: Searching on Google with Firefox

# import webdriver
from selenium import webdriver

# create webdriver object
driver = webdriver.Firefox()

# get google.co.in
driver.get("https://www.espn.com")
