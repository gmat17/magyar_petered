from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import time
import pandas as pd

class MajorOfficeScraper:
    def __init__(self):
        self.base_url = 'http://polgarmesterihivatal.helyek.eu'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def fetch_page(self, url):
        try:
            response = requests.get(url+'/telepules', headers=self.headers)
            response.raise_for_status()
        except Exception as e:
            print(f"Failed to retrieve {url}, skipping... ({e})")
            return None
        return response.content

    # Parse the main table page to extract links to detail pages
    def parse_table_page(self, html):
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find('table', class_='kethasab')
        if not table:
            print("Main table not found!")
            return []
        muni_list = table.find_all('a')
        href_list = [i.get('href') for i in muni_list if i.get('href')]
        return href_list

    # Parse each municipality detail page to extract name and address
    def parse_detail_page(self, href_list):
        data = []
        for href_elem in tqdm(href_list, desc="Scraping pages", unit="page"):
            time.sleep(0.4)
            page_html = self.fetch_page(self.base_url + href_elem)
            if not page_html:
                continue

            soup = BeautifulSoup(page_html, "html.parser")
            res_list = soup.find_all('div', class_='kereses_eredmeny')
            for res in res_list:
                links = res.find_all('a')
                if len(links) >= 2:
                    name = links[0].text.strip()
                    address = links[1].text.strip()
                    data.append([href_elem, name, address])
        return data

if __name__ == "__main__":
    scraper = MajorOfficeScraper()
    main_page_html = scraper.fetch_page(scraper.base_url)
    if main_page_html:
        hrefs = scraper.parse_table_page(main_page_html)
        results = scraper.parse_detail_page(hrefs)
        df=pd.DataFrame(results, columns=['href', 'name', 'address'])
        df.to_csv('mayor_offices.csv', index=False)
        print(f'{len(df)} records saved to mayor_offices.csv')# -*- coding: utf-8 -*-
"""
@gmat17
Created on Thu Jun  6 10:00:00 2024 
"""
