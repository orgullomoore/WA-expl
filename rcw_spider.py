import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import random
import logging
import sys
import os
import re
from urllib.parse import urljoin

# --- Configuration ---
DB_NAME = "wa_caselaw.db"
LOG_FILE = "spider_run.log"
BASE_URL = "https://app.leg.wa.gov/RCW/default.aspx"

# --- Logging Setup ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

# FIX 1: Use mode='a' (append) so we can read history for resuming
# and don't lose data on crash.
file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# FIX 2: Ensure console handles encoding gracefully to prevent OSError on Windows
console_handler = logging.StreamHandler(sys.stdout)
try:
    # Python 3.7+ supports reconfiguring streams, otherwise rely on default
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass 
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

class RCWSpider:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9", 
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        
        # State variables for resuming
        self.target_title = None
        self.target_chapter = None
        self.resume_mode = False
        
        # FIX 3: Add timeout to SQLite connection to reduce "database locked" errors
        logging.info(f"Connecting to database: {DB_NAME}")
        self.conn = sqlite3.connect(DB_NAME, timeout=30.0)
        self.cursor = self.conn.cursor()
        self._init_db()
        
        # Determine start point
        self.recover_last_state()

    def _init_db(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS statutes (
                citation TEXT PRIMARY KEY, title_num TEXT, chapter_num TEXT, section_num TEXT,
                url TEXT, full_text TEXT, crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
        self.conn.commit()

    def recover_last_state(self):
        """Parses the log file to find where we left off."""
        if not os.path.exists(LOG_FILE):
            logging.info("No log file found. Starting fresh.")
            return

        logging.info("Reading log file to recover state...")
        last_title = None
        last_chapter = None

        # Patterns based on the logging messages in crawl_titles and crawl_chapters
        # Pattern 1: Found Title 1. Drilling down...
        # Pattern 2: Found Chapter 1.04. Drilling down...
        title_pattern = re.compile(r"Found (Title .*?)\. Drilling down")
        chapter_pattern = re.compile(r"Found Chapter (.*?)\. Drilling down")

        try:
            with open(LOG_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    t_match = title_pattern.search(line)
                    if t_match:
                        last_title = t_match.group(1).strip()
                        # Reset chapter when a new title starts to avoid confusion
                        last_chapter = None 
                    
                    c_match = chapter_pattern.search(line)
                    if c_match:
                        last_chapter = c_match.group(1).strip()
        except Exception as e:
            logging.error(f"Error parsing log file: {e}")
            return

        if last_title:
            self.target_title = last_title
            self.resume_mode = True
            logging.info(f"RESUMING: Will fast-forward to {self.target_title}")
        
        if last_chapter:
            self.target_chapter = last_chapter
            logging.info(f"RESUMING: Will fast-forward to Chapter {self.target_chapter}")

    def _sleep(self):
        sleep_time = random.uniform(0.5, 2.0) # Reduced slightly for efficiency
        time.sleep(sleep_time)

    def fetch(self, url):
        self._sleep()
        retries = 3
        for i in range(retries):
            try:
                response = self.session.get(url, headers=self.headers, timeout=15)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                logging.warning(f"Fetch attempt {i+1} failed for {url}: {e}")
                time.sleep(2 * (i+1))
        logging.error(f"Failed to fetch {url} after {retries} attempts.")
        return None

    def crawl_titles(self):
        logging.info("Step 1: Fetching Main Page...")
        html = self.fetch(BASE_URL)
        if not html: return
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find("table", id="ContentPlaceHolder1_dgSections")
        if not table: return

        logging.info("Step 2: Parsing Titles...")
        rows = table.find_all("tr")
        for row in rows:
            link = row.find("a")
            if link and ("Title" in link.text or "Cite=" in link.get('href', '')):
                title_text = link.text.strip()
                
                # --- RESUME LOGIC FOR TITLES ---
                if self.resume_mode and self.target_title:
                    if title_text != self.target_title:
                        # Skip until we find the target
                        continue
                    else:
                        logging.info(f"Resumed at {title_text}.")
                        # We found the title, now we need to find the chapter inside it.
                        # We do NOT turn off resume_mode yet, because we need to find the chapter.
                        # However, once we finish this title, we must clear target_title 
                        # so next iterations process normally.
                
                full_url = urljoin(BASE_URL, link['href'])
                logging.info(f"Found {title_text}. Drilling down...")
                self.crawl_chapters(full_url, title_text)
                
                # Once we finish the loop for the target title, we turn off title skipping
                if self.resume_mode and title_text == self.target_title:
                    self.target_title = None

    def crawl_chapters(self, url, title_name):
        # logging.info(f"Step 3: Parsing Chapters for {title_name}...") 
        # (Commented out to reduce log noise, relied on "Drilling down" msg)
        html = self.fetch(url)
        if not html: return
        soup = BeautifulSoup(html, 'html.parser')
        content_wrapper = soup.find("div", id="contentWrapper")
        if not content_wrapper: return
        chapter_table = content_wrapper.find("table")
        if not chapter_table: return
        
        links = chapter_table.find_all("a")
        for link in links:
            text = link.text.strip()
            href = link.get('href', '')
            is_chapter_link = ("cite=" in href.lower() and text.count('.') == 1 and len(text) < 10 and "search" not in text.lower())
            
            if is_chapter_link:
                # --- RESUME LOGIC FOR CHAPTERS ---
                if self.resume_mode and self.target_chapter:
                    if text != self.target_chapter:
                        continue
                    else:
                        logging.info(f"Resumed at Chapter {text}.")
                        # Found the chapter. We can turn off resume_mode entirely now
                        # because within a chapter we rely on the DB check to skip sections.
                        self.resume_mode = False
                        self.target_chapter = None
                        self.target_title = None

                full_url = urljoin(BASE_URL, href)
                logging.info(f"Found Chapter {text}. Drilling down...")
                self.crawl_sections(full_url, title_name, text)

    def crawl_sections(self, url, title_name, chapter_num):
        # logging.info(f"Step 4: Parsing Sections for Chapter {chapter_num}...")
        html = self.fetch(url)
        if not html: return
        soup = BeautifulSoup(html, 'html.parser')
        content_wrapper = soup.find("div", id="contentWrapper")
        if not content_wrapper: return
        section_table = content_wrapper.find("table")
        if not section_table: return

        html_buttons = section_table.find_all("a", string="HTML")
        
        for btn in html_buttons:
            href = btn.get('href')
            full_url = urljoin(BASE_URL, href)
            try:
                citation = href.split("cite=")[1].split("&")[0]
            except IndexError:
                continue

            # Check DB to prevent re-work (Works in tandem with Resume Logic)
            try:
                self.cursor.execute("SELECT citation FROM statutes WHERE citation = ?", (citation,))
                if self.cursor.fetchone():
                    # logging.info(f"Skipping {citation} (Already in DB)") # Optional: reduce noise
                    continue
            except sqlite3.Error:
                pass # Proceed to try fetch/save if DB check fails momentarily

            self.extract_statute_content(full_url, title_name, chapter_num, citation)
    
    def extract_statute_content(self, url, title_name, chapter_num, citation):
        html = self.fetch(url)
        if not html: return
        soup = BeautifulSoup(html, 'html.parser')
        
        content_div = soup.find("div", id="contentWrapper")
        if content_div:
            text_parts = []
            for element in content_div.find_all(recursive=False):
                if element.name == 'h3' and 'NOTES:' in element.get_text():
                    break
                text_parts.append(element.get_text(separator="\n", strip=True))
            
            full_text = "\n".join(text_parts)
            # logging.info(f"Extracted {len(full_text)} chars of text for {citation}.")
            self.save_to_db(citation, title_name, chapter_num, citation, url, full_text)
        else:
            logging.error(f"Could not find 'contentWrapper' on final page for {citation}.")

    def save_to_db(self, citation, title, chapter, section, url, text):
        # FIX 4: Robust retry loop for "Database Locked" errors
        max_retries = 5
        for i in range(max_retries):
            try:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO statutes (citation, title_num, chapter_num, section_num, url, full_text)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (citation, title, chapter, section, url, text))
                self.conn.commit()
                logging.info(f"SUCCESS: Saved {citation}")
                return # Exit on success
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    wait = (i + 1) * 2
                    logging.warning(f"Database locked. Retrying {citation} in {wait}s...")
                    time.sleep(wait)
                else:
                    logging.error(f"Database error saving {citation}: {e}")
                    return
            except sqlite3.Error as e:
                logging.error(f"Database error saving {citation}: {e}")
                return

    def close(self):
        logging.info("Closing database connection.")
        try:
            self.conn.close()
        except:
            pass

if __name__ == "__main__":
    logging.info("=== RCW Spider Full Run Started ===")
    spider = RCWSpider()
    try:
        spider.crawl_titles() 
    except KeyboardInterrupt:
        logging.warning("Spider stopped by user.")
    except Exception as e:
        logging.critical(f"Unhandled Exception: {e}", exc_info=True)
    finally:
        spider.close()
        logging.info("=== RCW Spider Full Run Finished ===")