import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import json
from pathlib import Path
from typing import List, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import csv
from datetime import datetime
import re

class WebScraper:
    def __init__(self, base_url: str, delay: float = 1.0):
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.visited_urls: Set[str] = set()
        self.lock = threading.Lock()
        self.data = []
    
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"获取页面失败 {url}: {e}")
            return None
    
    def extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            if urlparse(full_url).netloc == urlparse(base_url).netloc:
                links.append(full_url)
        return links
    
    def extract_text(self, soup: BeautifulSoup, selectors: List[str]) -> Dict[str, str]:
        data = {}
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                data[selector] = element.get_text(strip=True)
        return data
    
    def crawl(self, max_pages: int = 10, 
              selectors: List[str] = None) -> List[Dict]:
        if selectors is None:
            selectors = ['title', 'h1', 'p']
        
        urls_to_visit = [self.base_url]
        
        while urls_to_visit and len(self.visited_urls) < max_pages:
            url = urls_to_visit.pop(0)
            
            if url in self.visited_urls:
                continue
            
            with self.lock:
                self.visited_urls.add(url)
            
            print(f"正在爬取: {url}")
            
            soup = self.fetch_page(url)
            if not soup:
                continue
            
            data = {
                'url': url,
                'title': soup.title.string if soup.title else '',
                'timestamp': datetime.now().isoformat()
            }
            
            extracted_data = self.extract_text(soup, selectors)
            data.update(extracted_data)
            
            self.data.append(data)
            
            links = self.extract_links(soup, url)
            urls_to_visit.extend(links)
            
            time.sleep(self.delay)
        
        return self.data
    
    def save_to_json(self, filename: str):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
    
    def save_to_csv(self, filename: str):
        if not self.data:
            return
        
        fieldnames = set()
        for item in self.data:
            fieldnames.update(item.keys())
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(fieldnames))
            writer.writeheader()
            writer.writerows(self.data)

class AdvancedWebScraper(WebScraper):
    def __init__(self, base_url: str, delay: float = 1.0):
        super().__init__(base_url, delay)
        self.scraped_data = []
    
    def extract_images(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        images = []
        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                full_url = urljoin(base_url, src)
                images.append({
                    'url': full_url,
                    'alt': img.get('alt', ''),
                    'width': img.get('width', ''),
                    'height': img.get('height', '')
                })
        return images
    
    def extract_tables(self, soup: BeautifulSoup) -> List[List[List[str]]]:
        tables = []
        for table in soup.find_all('table'):
            rows = []
            for row in table.find_all('tr'):
                cells = []
                for cell in row.find_all(['td', 'th']):
                    cells.append(cell.get_text(strip=True))
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables
    
    def extract_forms(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        forms = []
        for form in soup.find_all('form'):
            form_data = {
                'action': urljoin(base_url, form.get('action', '')),
                'method': form.get('method', 'GET'),
                'fields': []
            }
            
            for input_field in form.find_all('input'):
                form_data['fields'].append({
                    'type': input_field.get('type', 'text'),
                    'name': input_field.get('name', ''),
                    'value': input_field.get('value', '')
                })
            
            forms.append(form_data)
        
        return forms
    
    def crawl_parallel(self, max_pages: int = 10, 
                       max_workers: int = 4) -> List[Dict]:
        urls_to_visit = [self.base_url]
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            while urls_to_visit and len(self.visited_urls) < max_pages:
                batch = urls_to_visit[:max_workers]
                urls_to_visit = urls_to_visit[max_workers:]
                
                for url in batch:
                    if url in self.visited_urls:
                        continue
                    
                    with self.lock:
                        self.visited_urls.add(url)
                    
                    future = executor.submit(self._process_page, url)
                    futures.append(future)
                
                for future in as_completed(futures):
                    try:
                        data = future.result()
                        if data:
                            self.scraped_data.append(data)
                            links = data.get('links', [])
                            urls_to_visit.extend(links)
                    except Exception as e:
                        print(f"处理页面时出错: {e}")
                
                futures.clear()
                time.sleep(self.delay)
        
        return self.scraped_data
    
    def _process_page(self, url: str) -> Optional[Dict]:
        print(f"正在爬取: {url}")
        
        soup = self.fetch_page(url)
        if not soup:
            return None
        
        data = {
            'url': url,
            'title': soup.title.string if soup.title else '',
            'timestamp': datetime.now().isoformat(),
            'links': self.extract_links(soup, url),
            'images': self.extract_images(soup, url),
            'tables': self.extract_tables(soup),
            'forms': self.extract_forms(soup, url)
        }
        
        return data

class DataExtractor:
    @staticmethod
    def extract_emails(text: str) -> List[str]:
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        return re.findall(email_pattern, text)
    
    @staticmethod
    def extract_phone_numbers(text: str) -> List[str]:
        phone_patterns = [
            r'\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',
            r'\+86[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}[-.\s]?[0-9]{4}',
            r'[0-9]{3}[-.\s]?[0-9]{4}[-.\s]?[0-9]{4}'
        ]
        
        phones = []
        for pattern in phone_patterns:
            phones.extend(re.findall(pattern, text))
        
        return list(set(phones))
    
    @staticmethod
    def extract_urls(text: str) -> List[str]:
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        return re.findall(url_pattern, text)
    
    @staticmethod
    def extract_prices(text: str) -> List[str]:
        price_patterns = [
            r'¥\s*\d+(?:,\d{3})*(?:\.\d{2})?',
            r'\$\s*\d+(?:,\d{3})*(?:\.\d{2})?',
            r'€\s*\d+(?:,\d{3})*(?:\.\d{2})?',
            r'\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:元|美元|欧元)'
        ]
        
        prices = []
        for pattern in price_patterns:
            prices.extend(re.findall(pattern, text))
        
        return list(set(prices))
    
    @staticmethod
    def extract_dates(text: str) -> List[str]:
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{4}年\d{1,2}月\d{1,2}日',
            r'\d{1,2}月\d{1,2}日'
        ]
        
        dates = []
        for pattern in date_patterns:
            dates.extend(re.findall(pattern, text))
        
        return list(set(dates))

class APIClient:
    def __init__(self, base_url: str, api_key: str = None):
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            })
    
    def get(self, endpoint: str, params: Dict = None) -> Dict:
        url = urljoin(self.base_url, endpoint)
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def post(self, endpoint: str, data: Dict = None) -> Dict:
        url = urljoin(self.base_url, endpoint)
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def put(self, endpoint: str, data: Dict = None) -> Dict:
        url = urljoin(self.base_url, endpoint)
        response = self.session.put(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def delete(self, endpoint: str) -> Dict:
        url = urljoin(self.base_url, endpoint)
        response = self.session.delete(url)
        response.raise_for_status()
        return response.json()
    
    def paginate(self, endpoint: str, 
                page_param: str = 'page',
                per_page: int = 100) -> List[Dict]:
        all_data = []
        page = 1
        
        while True:
            params = {page_param: page, 'per_page': per_page}
            response = self.get(endpoint, params)
            
            if not response:
                break
            
            all_data.extend(response)
            
            if len(response) < per_page:
                break
            
            page += 1
        
        return all_data

class SocialMediaScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def scrape_twitter_profile(self, username: str) -> Dict:
        url = f"https://twitter.com/{username}"
        response = self.session.get(url)
        
        data = {
            'username': username,
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'followers': 0,
            'following': 0,
            'tweets': 0
        }
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        followers_elem = soup.select_one('[data-testid="followers"]')
        if followers_elem:
            data['followers'] = self._parse_number(followers_elem.get_text())
        
        following_elem = soup.select_one('[data-testid="following"]')
        if following_elem:
            data['following'] = self._parse_number(following_elem.get_text())
        
        return data
    
    def _parse_number(self, text: str) -> int:
        text = text.strip()
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
        
        for suffix, multiplier in multipliers.items():
            if suffix in text:
                number = float(text.replace(suffix, '').replace(',', ''))
                return int(number * multiplier)
        
        return int(text.replace(',', ''))
