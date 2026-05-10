import argparse
import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


SOURCE_NAME = "goud"
BASE_URL = "https://goud.ma/"
DEFAULT_TIMEOUT_SECONDS = 20


@dataclass
class Article:
    titre: str
    url: str
    date_publication: Optional[str]
    categorie: Optional[str]
    auteur: Optional[str]
    contenu: str
    source: str


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_valid_http_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in {"http", "https"} and bool(p.netloc)
    except Exception:
        return False


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _requests_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,fr-FR;q=0.9,fr;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }
    )
    return s


def fetch_html(session: requests.Session, url: str, timeout: int) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def extract_article_urls_from_homepage(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    base_host = urlparse(base_url).netloc

    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue

        abs_url = urljoin(base_url, href)
        if not _is_valid_http_url(abs_url):
            continue

        if urlparse(abs_url).netloc != base_host:
            continue

        if re.search(r"/(?:tag|tags|author|authors|category|categorie|video|videos|page)/", abs_url, flags=re.IGNORECASE):
            continue

        if not re.search(r"/\d{4}/\d{2}/\d{2}/", abs_url) and not re.search(r"\.(?:html|htm)(?:\?|#|$)", abs_url, flags=re.IGNORECASE):
            continue

        if abs_url in seen:
            continue
        seen.add(abs_url)
        urls.append(abs_url)

    return urls


def _extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("h1")
    if h1:
        t = _normalize_whitespace(h1.get_text(" ", strip=True))
        if t:
            return t

    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        return _normalize_whitespace(og["content"])

    if soup.title and soup.title.get_text(strip=True):
        return _normalize_whitespace(soup.title.get_text(strip=True))

    return ""


def _extract_category(soup: BeautifulSoup) -> Optional[str]:
    for sel in [
        ".breadcrumb a",
        "nav.breadcrumb a",
        "a[rel='category tag']",
        ".post-categories a",
    ]:
        a = soup.select_one(sel)
        if a:
            v = _normalize_whitespace(a.get_text(" ", strip=True))
            if v:
                return v

    section = soup.select_one('meta[property="article:section"]')
    if section and section.get("content"):
        return _normalize_whitespace(section["content"])

    return None


def _extract_author(soup: BeautifulSoup) -> Optional[str]:
    author_tag = soup.select_one("author")
    if author_tag:
        t = _normalize_whitespace(author_tag.get_text(" ", strip=True))
        if t:
            return t

    for sel in [
        'meta[name="author"]',
        'meta[property="article:author"]',
    ]:
        m = soup.select_one(sel)
        if m and m.get("content"):
            t = _normalize_whitespace(str(m["content"]))
            if t:
                return t

    for sel in [
        ".author",
        ".post-author",
        "a[rel='author']",
        ".td-post-author-name",
        ".entry-author",
    ]:
        el = soup.select_one(sel)
        if el:
            t = _normalize_whitespace(el.get_text(" ", strip=True))
            if t:
                return t

    return None


def _extract_published_date_iso(soup: BeautifulSoup) -> Optional[str]:
    time_tag = soup.select_one("time[datetime]")
    if time_tag and time_tag.get("datetime"):
        return _normalize_whitespace(time_tag["datetime"])

    meta_time = soup.select_one('meta[property="article:published_time"]')
    if meta_time and meta_time.get("content"):
        return _normalize_whitespace(meta_time["content"])

    time_any = soup.select_one("time")
    if time_any:
        raw = _normalize_whitespace(time_any.get_text(" ", strip=True))
        if raw:
            return raw

    return None


def _extract_text_from_article_body(soup: BeautifulSoup) -> str:
    candidates = [
        soup.select_one("article"),
        soup.select_one(".post-content"),
        soup.select_one(".entry-content"),
        soup.select_one(".article-content"),
        soup.select_one(".td-post-content"),
    ]

    for c in candidates:
        if not c:
            continue
        ps = c.select("p")
        text = _normalize_whitespace(" ".join(p.get_text(" ", strip=True) for p in ps))
        if len(text) >= 50:
            return text

    ps = soup.select("p")
    return _normalize_whitespace(" ".join(p.get_text(" ", strip=True) for p in ps))


def parse_article(html: str, url: str) -> Article:
    soup = BeautifulSoup(html, "html.parser")

    titre = _extract_title(soup)
    categorie = _extract_category(soup)
    date_publication = _extract_published_date_iso(soup)
    auteur = _extract_author(soup)
    contenu = _extract_text_from_article_body(soup)

    return Article(
        titre=titre,
        url=url,
        date_publication=date_publication,
        categorie=categorie,
        auteur=auteur,
        contenu=contenu,
        source=SOURCE_NAME,
    )


def validate_article(a: Article) -> list[str]:
    errors: list[str] = []

    if not a.titre.strip():
        errors.append("titre vide")
    if not a.date_publication or not str(a.date_publication).strip():
        errors.append("date manquante")
    if len(a.contenu.strip()) <= 100:
        errors.append("contenu trop court")
    if not _is_valid_http_url(a.url):
        errors.append("url invalide")

    return errors


def scrape_goud(
    listing_url: str,
    max_articles: int,
    sleep_seconds: float,
    timeout: int,
    user_agent: str,
) -> tuple[list[Article], dict]:
    session = _requests_session(user_agent=user_agent)

    listing_html = fetch_html(session, listing_url, timeout=timeout)
    urls = extract_article_urls_from_homepage(listing_html, base_url=BASE_URL)
    urls = urls[:max_articles]

    articles: list[Article] = []
    invalid: list[dict] = []

    for i, url in enumerate(urls, start=1):
        try:
            html = fetch_html(session, url, timeout=timeout)
            art = parse_article(html, url)
            errs = validate_article(art)
            if errs:
                invalid.append({"url": url, "errors": errs})
            else:
                articles.append(art)
        except Exception as e:
            invalid.append({"url": url, "errors": [f"exception: {type(e).__name__}: {e}"]})

        if sleep_seconds > 0 and i < len(urls):
            time.sleep(sleep_seconds)

    meta = {
        "source": SOURCE_NAME,
        "listing_url": listing_url,
        "scraped_at": _now_utc_iso(),
        "requested_max_articles": max_articles,
        "found_urls": len(urls),
        "valid_articles": len(articles),
        "invalid_articles": len(invalid),
        "invalid_details": invalid,
    }

    return articles, meta


def write_json(output_path: str, articles: Iterable[Article], meta: dict) -> None:
    payload = {
        "meta": meta,
        "articles": [asdict(a) for a in articles],
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Scraper Goud -> JSON local")
    p.add_argument(
        "--listing-url",
        default=BASE_URL,
        help="URL de listing (homepage/section) depuis laquelle extraire des URLs d'articles.",
    )
    p.add_argument(
        "--max-articles",
        type=int,
        default=20,
        help="Nombre max d'articles à scraper.",
    )
    p.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.8,
        help="Pause entre requêtes (anti-bannissement).",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Timeout HTTP en secondes.",
    )
    p.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
        help="User-Agent à utiliser.",
    )
    p.add_argument(
        "--output",
        default="goud_articles.json",
        help="Chemin du fichier JSON de sortie.",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    articles, meta = scrape_goud(
        listing_url=args.listing_url,
        max_articles=args.max_articles,
        sleep_seconds=args.sleep_seconds,
        timeout=args.timeout,
        user_agent=args.user_agent,
    )

    write_json(args.output, articles, meta)

    print(f"OK - {meta['valid_articles']} articles valides écrits dans {args.output}")
    if meta["invalid_articles"]:
        print(f"WARN - {meta['invalid_articles']} articles ignorés (validation/erreurs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
