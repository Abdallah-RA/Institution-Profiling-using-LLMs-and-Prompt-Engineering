"""
data_collector_async.py  –  bug‑fixed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ultra‑fast, fully‑async collector (≈1–1.5 s round‑trip).

Sources
-------
wikipedia | wikidata | news | duckduckgo | dbpedia | search | homepage
"""

from __future__ import annotations
import asyncio, logging, re, textwrap
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from urllib.parse import quote_plus, urlparse

import aiohttp
from bs4 import BeautifulSoup


# ----------- tunables ------------------------------------------------------
_MAX_CHARS   = 4_096
_TIMEOUT     = aiohttp.ClientTimeout(total=6)
_POOL_SIZE   = 20
_TLDS        = ("edu", "gov", "ac", "org", "com", "net", "tech")
_RECENT_DAYS = 365
_HOME_TRIES  = 6
_HOME_TO     = 2


# ----------- simple FIFO cache (stores final strings, not Tasks) ----------
class FIFOCache(dict):
    def __init__(self, cap: int = 512):
        super().__init__()
        self.cap = cap

    def put(self, k, v):
        if k not in self and len(self) >= self.cap:
            self.pop(next(iter(self)))
        self[k] = v


def _snippet_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    if meta and meta.get("content"):
        return meta["content"].strip()
    p = soup.find("p")
    return p.get_text(" ", strip=True) if p else ""


def _extract_date(text: str) -> Optional[datetime]:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except ValueError:
            pass
    return None


# ----------- async collector ---------------------------------------------
@dataclass
class DataCollector:
    sources: List[str] = field(
        default_factory=lambda: ["wikipedia", "search", "homepage"]
    )
    max_chars: int = _MAX_CHARS
    cache_cap: int = 512

    _cache: FIFOCache = field(init=False)
    _fetchers: Dict[str, callable] = field(init=False)

    def __post_init__(self):
        self._cache = FIFOCache(self.cache_cap)
        self._fetchers = {
            "wikipedia":  self._fetch_wiki,
            "wikidata":   self._fetch_wikidata,
            "news":       self._fetch_news,
            "duckduckgo": self._fetch_ddg,
            "dbpedia":    self._fetch_dbpedia,
            "search":     self._fetch_search,
            "homepage":   self._fetch_home,
        }

    # ---------- public sync entry ----------------------------------------
    def collect(self, inst: str) -> str:
        return asyncio.run(self._collect(inst))

    # ---------- orchestrator (async) -------------------------------------
    async def _collect(self, inst: str) -> str:
        conn = aiohttp.TCPConnector(limit=_POOL_SIZE, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=conn, timeout=_TIMEOUT) as sess:
            coros = [self._fetchers[s](sess, inst) for s in self.sources if s in self._fetchers]
            results = await asyncio.gather(*coros, return_exceptions=True)

        chunks, seen = [], set()
        for res in results:
            if isinstance(res, Exception):
                logging.warning("fetcher error: %s", res)
                continue
            for line in filter(None, map(str.strip, res.splitlines())):
                if line not in seen:
                    seen.add(line)
                    chunks.append(line)

        merged = "\n".join(chunks)
        if len(merged) > self.max_chars:
            merged = textwrap.shorten(merged, self.max_chars, placeholder=" …")
        return merged

    # ---------- individual fetchers (await and cache STRING) -------------
    async def _fetch_wiki(self, s, inst):
        k = ("wiki", inst.lower())
        if k in self._cache:
            return self._cache[k]
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote_plus(inst)}"
        async with s.get(url) as r:
            if r.status == 404:
                txt = ""
            else:
                txt = (await r.json()).get("extract", "")
        self._cache.put(k, txt)
        return txt

    async def _fetch_wikidata(self, s, inst):
        k = ("wikidata", inst.lower())
        if k in self._cache:
            return self._cache[k]
        url = ("https://www.wikidata.org/wiki/Special:EntityData/"
               f"{quote_plus(inst)}.json?flavor=simple")
        async with s.get(url) as r:
            if r.status == 404:
                txt = ""
            else:
                ent = next(iter((await r.json()).get("entities", {}).values()), {})
                txt = ent.get("descriptions", {}).get("en", {}).get("value", "")
        self._cache.put(k, txt)
        return txt

    async def _fetch_news(self, s, inst, k: int = 5):
        key = ("news", inst.lower())
        if key in self._cache:
            return self._cache[key]
        url = ("https://news.google.com/rss/search?q="
               f"{quote_plus(inst)}&hl=en&gl=US&ceid=US:en")
        async with s.get(url) as r:
            xml = await r.text()
        heads = [i.title.text for i in BeautifulSoup(xml, "xml").find_all("item", limit=k)]
        txt = " ".join(heads)
        self._cache.put(key, txt)
        return txt

    async def _fetch_ddg(self, s, inst):
        k = ("ddg", inst.lower())
        if k in self._cache:
            return self._cache[k]
        url = (f"https://api.duckduckgo.com/?q={quote_plus(inst)}&format=json"
               "&no_redirect=1&no_html=1")
        async with s.get(url) as r:
            data = await r.json(content_type=None)
        txt = data.get("Abstract") or (data.get("RelatedTopics") or [{}])[0].get("Text", "")
        self._cache.put(k, txt)
        return txt

    async def _fetch_dbpedia(self, s, inst):
        k = ("dbpedia", inst.lower())
        if k in self._cache:
            return self._cache[k]
        slug = quote_plus(inst.replace(" ", "_"))
        url = ("https://dbpedia.org/sparql?query="
               "SELECT+?abs+WHERE+{+dbr:%s+dbo:abstract+?abs+."
               "FILTER(lang(?abs)%%3D'en')+}+LIMIT+1&format=json" % slug)
        async with s.get(url) as r:
            if r.status != 200:
                txt = ""
            else:
                bindings = (await r.json(content_type=None))["results"]["bindings"]
                txt = bindings[0]["abs"]["value"] if bindings else ""
        self._cache.put(k, txt)
        return txt

    # ---------- search: parallel page fetch w/ freshness ------------------
    async def _fetch_search(self, s, inst, keep: int = 8):
        k = ("search", inst.lower())
        if k in self._cache:
            return self._cache[k]

        serp_url = f"https://duckduckgo.com/html/?q={quote_plus(inst)}"
        async with s.get(serp_url) as r:
            html = await r.text()
        links = [
            a["href"]
            for a in BeautifulSoup(html, "html.parser").select("a.result__a")[:20]
            if any((urlparse(a["href"]).hostname or "").endswith("." + t) for t in _TLDS)
        ]

        sem = asyncio.Semaphore(10)
        snippets = []

        async def grab(u):
            async with sem:
                try:
                    async with s.get(u, timeout=4) as resp:
                        page = await resp.text()
                    if _extract_date(page) and (datetime.utcnow() - _extract_date(page)).days > _RECENT_DAYS:
                        return ""
                    return _snippet_from_html(page)
                except Exception:
                    return ""

        tasks = [asyncio.create_task(grab(u)) for u in links]
        for fut in asyncio.as_completed(tasks):
            snip = await fut
            if snip:
                snippets.append(snip)
            if len(snippets) >= keep:
                break

        txt = "\n".join(snippets)
        self._cache.put(k, txt)
        return txt

    # ---------- homepage fast probe ---------------------------------------
    async def _fetch_home(self, s, inst):
        k = ("home", inst.lower())
        if k in self._cache:
            return self._cache[k]

        def cand(name):
            clean = re.sub(r"[^\w]", " ", name).lower().split()
            base = "".join(clean)
            roots = [base] + ([base[:-10]] if base.endswith("university") else [])
            return [f"{r}.{t}" for r in roots for t in _TLDS][: _HOME_TRIES]

        async def probe(dom):
            for scheme in ("https://", "http://"):
                try:
                    async with s.head(scheme + dom, timeout=_HOME_TO, allow_redirects=True) as h:
                        if h.status >= 400:
                            continue
                    async with s.get(scheme + dom, timeout=_HOME_TO) as g:
                        if g.status == 200:
                            snip = _snippet_from_html(await g.text())
                            if snip:
                                return snip
                except Exception:
                    continue
            return ""

        tasks = [asyncio.create_task(probe(d)) for d in cand(inst)]
        for fut in asyncio.as_completed(tasks):
            snip = await fut
            if snip:
                for t in tasks:
                    t.cancel()
                self._cache.put(k, snip)
                return snip

        # fallback first SERP result
        try:
            url = f"https://duckduckgo.com/html/?q={quote_plus(inst+' official site')}"
            async with s.get(url, timeout=4) as r:
                html = await r.text()
            link = BeautifulSoup(html, "html.parser").select_one("a.result__a")
            if link and "href" in link.attrs:
                async with s.get(link["href"], timeout=4) as r2:
                    snip = _snippet_from_html(await r2.text())
                    self._cache.put(k, snip)
                    return snip
        except Exception:
            pass
        self._cache.put(k, "")
        return ""
