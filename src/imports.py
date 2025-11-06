import os
import math
import json
import time

import pandas as pd
import numpy as np

import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy import types

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import yfinance as yf

from dotenv import load_dotenv
from time import sleep
from datetime import datetime

from tenacity import retry, stop_after_attempt, wait_exponential