import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from imports import *

class Service:
    @classmethod
    def initialize(cls):
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting B3 Scraper...")
        try:
            scraper_path = os.path.join(os.path.dirname(__file__), 'app', 'scraper.py')
            subprocess.run([sys.executable, scraper_path], check=True)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraper completed!")
        except Exception as e:
            print(f"Scraper error: {e}")