import pandas as pd
import numpy as np

import requests
from hashlib import sha224
from pathlib import Path
from bs4 import BeautifulSoup
from typing import Set
import bz2
from urllib.parse import urlparse
import json
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import sys
from loguru import logger
import random
sys.setrecursionlimit(10**6) 

HTMLS_DIR = Path(__file__).resolve().parent.parent / "var/htmls"
LINKS_DIR = Path(__file__).resolve().parent.parent / "var/links"

PROXY_CSV = Path(__file__).resolve().parent.parent / "var/proxy.csv"
proxy = pd.read_csv(PROXY_CSV, sep=":")
proxy.columns = ["url", "port", "username", "password"]
proxies = []
for url, port, username, password in zip(proxy.url, proxy.port, proxy.username, proxy.password):
    proxies.append( dict(http=f"http://{username}:{password}@{url}:{port}", 
        https=f"http://{username}:{password}@{url}:{port}") )


def get_digest(x):
    return sha224(bytes(x, "utf8")).hexdigest()[:24]

def get(url) -> Set[str]:
    try:
        d = get_digest(url)
        if (LINKS_DIR / d).exists():
            with bz2.open(LINKS_DIR / d, "rt") as fp:
                ret = set(json.load(fp))
            return ret

        with requests.get(url, proxies=random.choice(proxies)) as r:
            html = r.text
            # print(html)

        soup = BeautifulSoup(r.text, "lxml")
        try:
            soup.html.body.insert(-1, BeautifulSoup(f'<original_url value="{url}" />', "lxml"))
        except Exception as exc:
            # print(exc)
            # print(html)
            pass
        for remove_tag in ["script", "style"]:
            for tgt in soup.find_all(remove_tag):
                tgt.extract()
        with bz2.open(HTMLS_DIR / d, "wt") as fp:
            fp.write(str(soup)) 

        
        ret_urls = set()
        for a in soup.find_all("a", {"href":True}):
            p = urlparse(a.get("href"))
            p = p._replace(scheme="https", netloc="www.boatrace.jp")
            url = p.geturl()
            if (HTMLS_DIR / get_digest(url)).exists():
                continue
            ret_urls.add(url)

        with bz2.open(LINKS_DIR / d, "wt") as fp:
            json.dump(list(ret_urls), fp)
        return ret_urls
    except Exception as exc:
        tb_lineno = sys.exc_info()[2].tb_lineno
        # logger.info(f"{tb_lineno}, {exc}")
        return set()

urls = set()
for chunk in tqdm(LINKS_DIR.glob("*")):
    with bz2.open(chunk, "rt") as fp:
        urls |= set(json.load(fp))
if urls == set():
    seed = "https://www.boatrace.jp/"
    urls = [seed]



for i in range(10):
    next_urls = set()
    with ProcessPoolExecutor(max_workers=400) as exe:
        for _next_urls in tqdm(exe.map(get, urls), total=len(urls)):
            next_urls |= _next_urls
    urls = next_urls
