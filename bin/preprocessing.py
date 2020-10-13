from dataclasses import dataclass, asdict
import requests
import pandas as pd
import numpy as np
from pathlib import Path
import bz2
from bs4 import BeautifulSoup
import re
from hashlib import sha224
from tqdm import tqdm
import json
from urllib.parse import urlparse
from typing import Optional

def get_digest(x):
    return sha224(bytes(x, "utf8")).hexdigest()[:24]


HTML_DIR = Path(__file__).resolve().parent.parent / "var/htmls"
LINKS_DIR = Path(__file__).resolve().parent.parent / "var/links"
TOP_DIR = Path(__file__).resolve().parent.parent


def is_detail_query(query):
    if "rno=" in query and "jcd=" in query and "hd=" in query:
        qp = dict([kv.split("=") for kv in query.split("&")])
        # print(qp)
        return query, qp
    return None


q_tp = dict()

@dataclass
class Sources:
    date: Optional[str] = None
    raceresult: Optional[str] = None
    odds3t: Optional[str] = None
    beforeinfo: Optional[str] = None

if not (TOP_DIR / "var/qtp.csv").exists():
    for chunk in tqdm(LINKS_DIR.glob("*")):
        with bz2.open(chunk, "rt") as fp:
            for link in json.load(fp):
                p = urlparse(link)
                if is_detail_query(p.query) is None:
                    continue
                q, qp = is_detail_query(p.query)
                if q not in q_tp:
                    q_tp[q] = Sources()
                q_tp[q].date = qp["hd"]
                # print(link, q_tp[q])            
                if "/odds3t?" in link and (HTML_DIR / get_digest(link)).exists():
                    q_tp[q].odds3t = link
                elif "/raceresult?" in link and (HTML_DIR / get_digest(link)).exists():
                    q_tp[q].raceresult = link
                elif "/beforeinfo?" in link and (HTML_DIR / get_digest(link)).exists():
                    q_tp[q].beforeinfo = link
                else:
                    continue
    df = pd.DataFrame([asdict(tp) for tp in q_tp.values()])
    df["q"] = pd.Series(list(q_tp.keys()))
    df.to_csv(TOP_DIR / "var/qtp.csv", index=None)

df = pd.read_csv(TOP_DIR / "var/qtp.csv")
for col in ["odds3t", "raceresult", "beforeinfo"]:
    df = df[pd.notnull(df[col])]

def parse_pages(q, raceresult, odds3t, beforeinfo):

    with bz2.open(HTML_DIR / get_digest(raceresult), "rt") as fp:
        soup = BeautifulSoup(fp.read(), "lxml")

    results = []
    for a in soup.find_all("tbody"):
        td = a.find_all("td")
        if len(td) != 4:
            continue

        chaku = td[0].text.strip()
        waku = td[1].text.strip()
        racer = td[2].find_all("span")[-1].text.strip()
        racer = re.sub("\s{1,}", " ", racer)
        time = td[3].text.strip()

        results.append([chaku, waku, racer, time])
    results = np.array(results)
    results = pd.DataFrame(results)
    if len(results) != 6:
        return None
    results.columns = ["chaku", "waku", "racer", "time"]
    results.sort_values(by=["waku"], inplace=True)
    # print(results)

    with bz2.open(HTML_DIR / get_digest(odds3t), "rt") as fp:
        soup = BeautifulSoup(fp.read(), "lxml")

    odds = []
    for o in soup.find_all("td", {"class": "oddsPoint"}):
        try:
            odds.append(float(o.text.strip()))
        except:
            odds.append(None)
    odds = np.array(odds)
    try:
        odds = odds.reshape(20, 6)
    except Exception as exc:
        # 中止等
        return None
    odds = pd.DataFrame(odds)
    odds.columns = [f"waku_{i}" for i in range(1, 7)]
    # print(odds)
    with bz2.open(HTML_DIR / get_digest(beforeinfo), "rt") as fp:
        soup = BeautifulSoup(fp.read(), "lxml")
    
    details = []
    for tbody in soup.find_all("tbody", {"class": "is-fs12"}):
        xs = [x.text for x in tbody.find_all("td")]
        # ['1', '', '白石\u3000\u3000\u3000健', '51.1kg', '6.70', '0.5', '\xa0', '\n\n\n', 'R', '2', '進入', '3', '0.0', 'ST', '.28', '着順', '１']
        waku = xs[0]
        name = re.sub("\s{1,}", " ", xs[2])
        weight = xs[3]
        tenji_time = xs[4]
        tilt = xs[5]
        details.append([waku, name, weight, tenji_time, tilt])
    details = pd.DataFrame(details)
    details.columns = ["waku", "name", "weight", "tenji_time", "tilt"]
    # print(details)

    df = pd.merge(results, details, on=["waku"], how="inner")
    df["q"] = q
    qparam = dict([x.split("=") for x in q.split("&")])
    df["date"] = qparam["hd"]
    df["rno"] = qparam["rno"]

    def stats(waku):
        mi = odds[f"waku_{waku}"].min()
        ma = odds[f"waku_{waku}"].max()
        mean = odds[f"waku_{waku}"].mean()
        median = odds[f"waku_{waku}"].median()
        return [waku, mi, ma, mean, median]
    a = pd.DataFrame(np.array(df.waku.apply(stats).tolist()))
    a.columns = ["waku", "mi", "ma", "mean", "median"]
    df = pd.merge(df, a, on=["waku"], how="inner")
    # print(a)
    # print(df)
    
    return df

subs = []
for i in tqdm(range(len(df))):
    r = df.iloc[i]
    try:
        sub = parse_pages(
                q=r.q,
                raceresult=r.raceresult,  
                odds3t=r.odds3t, 
                beforeinfo=r.beforeinfo)
    except Exception as exc:
        print(exc)
        continue
    subs.append(sub)
pd.concat(subs).to_csv(TOP_DIR / "var/preprocessed.csv", index=None)
