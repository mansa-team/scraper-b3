import os
import sys
import math
import json
import time
import subprocess
import gc

import pandas as pd
import numpy as np

from sqlalchemy import create_engine, text, types

import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from dotenv import load_dotenv
from time import sleep
from datetime import datetime

from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

class Config:
    MYSQL = {
        'HOST': os.getenv('MYSQL_HOST'),
        'USER': os.getenv('MYSQL_USER'),
        'PASSWORD': os.getenv('MYSQL_PASSWORD'),
        'DATABASE': os.getenv('MYSQL_DATABASE')
    }

    SCRAPER = {
        'ENABLED': os.getenv('SCRAPER_ENABLED'),
        'SCHEDULER': os.getenv('SCRAPER_SCHEDULER'),
        'JSON': os.getenv('JSON_EXPORT'),
        'MYSQL': os.getenv('MYSQL_EXPORT'),
        'MAX_WORKERS': os.getenv('MAX_WORKERS')
    }