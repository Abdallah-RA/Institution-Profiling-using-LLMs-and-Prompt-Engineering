"""
data_collector.py
~~~~~~~~~~~~~~~~~
Multi‑source context collector for LLM profiling.

Sources handled
---------------
wikipedia | wikidata | homepage | news | duckduckgo | dbpedia | search
"""

from __future__ import annotations
import logging
import re
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib.parse import quote_plus, urlparse
from urllib3.util.retry import Retry


# ---------- constants -------------------------------------------------------
_TLDS          = ("edu", "gov", "ac", "org", "com", "net", "tech")
_MAX_TRIES     = 6     # homepage domain probes
_PROBE_TIMEOUT = 2     # seconds per request
_THREADS       = 6     # parallel homepage probes
_RECENT_DAYS   = 365   # freshness window for 'search' snippets


@dataclass
class DataCollector:
    sources: List[str] = field(default_factory=lambda: ["wikipedia", "search"])
    timeout: int = 10
    max_chars: int = 4_096

    _FETCHERS: Dict[str, Callable[[str], str]] = field(init=False, repr=False)
    _MEMO_LIMIT: int = field(default=512, init=False, repr=False)
    _memo: Dict[Tuple[str, str], str] = field(default_factory=dict, init=False, repr=False)

    # --------------------------------------------------------------------- #
    def __post_init__(self):
        retry = Retry(total=3, backoff_factor=0.4,
                      status_forcelist=[500, 502, 503, 504],
                      allowed_methods=frozenset(["GET", "HEAD"]))
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

        self._FETCHERS = {
            "wikipedia":  self._fetch_wikipedia_summary,
            "wikidata":   self._fetch_wikidata_blurb,
            "homepage":   self._fetch_homepage_snippet,
            "news":       self._fetch_news_headlines,
            "duckduckgo": self._fetch_ddg_instant,
            "dbpedia":    self._fetch_dbpedia_abstract,
            "search":     self._fetch_search_snippets,     # NEW
        }

    # --------------------------------------------------------------------- #
    def collect(self, institution: str) -> str:
        chunks, seen = [], set()
        for src in self.sources:
            fn = self._FETCHERS.get(src)
            if not fn:
                logging.warning("Unknown source '%s' – skipping.", src)
                continue
            try:
                txt = fn(institution) or ""
                for line in filter(None, map(str.strip, txt.splitlines())):
                    if line not in seen:
                        seen.add(line)
                        chunks.append(line)
            except Exception as exc:
                logging.warning("[DataCollector] %s() failed: %s", src, exc)

        merged = "\n".join(chunks)
        if len(merged) > self.max_chars:
            merged = textwrap.shorten(merged, self.max_chars, placeholder=" …")
        return merged

    # ------------------- cache helper -------------------------------------
    def _memoize(self, key: Tuple[str, str], val: Optional[str] = None):
        if val is None:
            return self._memo.get(key)
        if len(self._memo) >= self._MEMO_LIMIT:
            self._memo.pop(next(iter(self._memo)))
        self._memo[key] = val
        return val

    # ------------------- standard fetchers --------------------------------
    def _fetch_wikipedia_summary(self, inst: str) -> str:
        k = ("wiki", inst.lower())
        if (c := self._memoize(k)) is not None:
            return c
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote_plus(inst)}"
        r = self.session.get(url, timeout=self.timeout)
        if r.status_code == 404:
            return ""
        r.raise_for_status()
        return self._memoize(k, r.json().get("extract", ""))

    def _fetch_wikidata_blurb(self, inst: str) -> str:
        k = ("wikidata", inst.lower())
        if (c := self._memoize(k)) is not None:
            return c
        url = "https://www.wikidata.org/wiki/Special:EntityData/" \
              f"{quote_plus(inst)}.json?flavor=simple"
        r = self.session.get(url, timeout=self.timeout)
        if r.status_code == 404:
            return ""
        r.raise_for_status()
        ent = next(iter(r.json().get("entities", {}).values()), {})
        desc = ent.get("descriptions", {}).get("en", {}).get("value", "")
        return self._memoize(k, desc)

    def _fetch_news_headlines(self, inst: str, items: int = 5) -> str:
        k = ("news", inst.lower())
        if (c := self._memoize(k)) is not None:
            return c
        url = f"https://news.google.com/rss/search?q={quote_plus(inst)}&hl=en&gl=US&ceid=US:en"
        r = self.session.get(url, timeout=self.timeout)
        soup = BeautifulSoup(r.content, "xml")
        heads = [i.title.text for i in soup.find_all("item", limit=items)]
        return self._memoize(k, " ".join(heads))

    def _fetch_ddg_instant(self, inst: str) -> str:
        k = ("ddg", inst.lower())
        if (c := self._memoize(k)) is not None:
            return c
        url = f"https://api.duckduckgo.com/?q={quote_plus(inst)}&format=json&no_redirect=1&no_html=1"
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        d = r.json()
        abstract = d.get("Abstract") or \
                   (d.get("RelatedTopics") or [{}])[0].get("Text", "")
        return self._memoize(k, abstract)

    def _fetch_dbpedia_abstract(self, inst: str) -> str:
        k = ("dbpedia", inst.lower())
        if (c := self._memoize(k)) is not None:
            return c
        slug = quote_plus(inst.replace(" ", "_"))
        sparql = (
            "https://dbpedia.org/sparql?query="
            "SELECT+?abs+WHERE+{+dbr:%s+dbo:abstract+?abs+."
            "FILTER(lang(?abs)%%3D'en')+}+LIMIT+1&format=json" % slug
        )
        r = self.session.get(sparql, timeout=self.timeout)
        if r.status_code != 200:
            return ""
        results = r.json().get("results", {}).get("bindings", [])
        abstract = results[0]["abs"]["value"] if results else ""
        return self._memoize(k, abstract)

    # ------------------- NEW: search fetcher ------------------------------
    def _fetch_search_snippets(self, inst: str, k: int = 8) -> str:
        """
        DuckDuckGo SERP → follow result pages → extract fresh, high‑quality snippets.
        """
        key = ("search", inst.lower())
        if (c := self._memoize(key)) is not None:
            return c

        serp = self.session.get(
            f"https://duckduckgo.com/html/?q={quote_plus(inst)}",
            timeout=6
        )
        soup = BeautifulSoup(serp.text, "html.parser")

        snippets, picked = [], 0
        for a in soup.select("a.result__a")[:20]:        # scan top 20 links
            url = a["href"]
            domain = urlparse(url).hostname or ""
            if not any(domain.endswith("." + t) or domain == f"{t}" for t in _TLDS):
                continue  # skip low‑quality TLDs

            try:
                page = self.session.get(url, timeout=4)
                txt  = self._snippet_from_html(page.text)
                date = self._extract_date(page.text)
                if date and (datetime.utcnow() - date).days > _RECENT_DAYS:
                    continue  # too old
                if txt:
                    snippets.append(txt)
                    picked += 1
                if picked >= k:
                    break
            except Exception:
                continue

        return self._memoize(key, "\n".join(snippets))

    # helpers for search
    @staticmethod
    def _extract_date(html: str):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", html)
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y-%m-%d")
            except ValueError:
                pass
        return None

    # ------------------------------------------------------------------ #
    # homepage helpers (parallel probe, unchanged)
    @staticmethod
    def _guess_domains(name: str) -> List[str]:
        clean = re.sub(r"[^\w]", " ", name).lower().split()
        base = "".join(clean)
        roots = [base]
        if base.endswith("university"):
            roots.append(base[:-10])
        return [f"{r}.{t}" for r in roots for t in _TLDS][: _MAX_TRIES]

    @staticmethod
    def _snippet_from_html(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        meta = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
        if meta and meta.get("content"):
            return meta["content"].strip()
        p = soup.find("p")
        return p.get_text(" ", strip=True) if p else ""

    def _fetch_homepage_snippet(self, inst: str) -> str:
        k = ("home", inst.lower())
        if (c := self._memoize(k)) is not None:
            return c

        def probe(domain: str) -> str:
            for scheme in ("https://", "http://"):
                try:
                    url = scheme + domain
                    h = self.session.head(url, timeout=_PROBE_TIMEOUT, allow_redirects=True)
                    if h.status_code >= 400:
                        continue
                    g = self.session.get(url, timeout=_PROBE_TIMEOUT)
                    if g.status_code == 200:
                        snip = self._snippet_from_html(g.text)
                        if snip:
                            return snip
                except Exception:
                    continue
            return ""

        domains = self._guess_domains(inst)

        with ThreadPoolExecutor(max_workers=_THREADS) as exe:
            futures = {exe.submit(probe, d): d for d in domains}
            for fut in as_completed(futures):
                snip = fut.result()
                if snip:
                    for f in futures:
                        f.cancel()
                    return self._memoize(k, snip)

        # fallback: DuckDuckGo first result
        try:
            query = quote_plus(f"{inst} official website")
            url = f"https://duckduckgo.com/html/?q={query}"
            r = self.session.get(url, timeout=_PROBE_TIMEOUT * 2)
            soup = BeautifulSoup(r.text, "html.parser")
            link = soup.select_one("a.result__a")
            if link and "href" in link.attrs:
                target = link["href"]
                r2 = self.session.get(target, timeout=_PROBE_TIMEOUT * 2)
                snip = self._snippet_from_html(r2.text)
                if snip:
                    return self._memoize(k, snip)
        except Exception:
            pass

        return ""
