#!/usr/bin/env python3
"""


Configuration:
 • USE_LLM_CLEANING — Toggle between LLM (True) or manual (False) cleaning
 • JUNK_SELECTORS — Add HTML elements/classes to remove during cleaning
 • URL_EXCLUDE_PATTERNS — Add URL patterns to hard exclude (never visit)
"""

import argparse
import asyncio
import hashlib
import json
import os
import random
import re
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin

import requests
from selectolax.parser import HTMLParser
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db.session import SessionLocal
from app.models import Blog, Post

# ------------------------------
# CLEANING METHOD CONFIGURATION
# ------------------------------
# Set to False to use manual HTML parsing instead of LLM
# LLM can hallucinate - manual parsing is more reliable but less sophisticated
USE_LLM_CLEANING = True  # Change to False if LLM adds unwanted content


# ------------------------------
# JUNK REMOVAL CONFIGURATION
# ------------------------------
# Add any problematic HTML elements/classes/IDs here to remove them during cleaning
# 
# Examples of what to add:
# - Specific classes: ".some-annoying-popup"
# - Specific IDs: "#some-ad-container"
# - Element types: "video", "audio"
# - Attribute patterns: "[data-ad]", "[class*='promo']"
#
# Current selectors will remove:
# - All images (img, picture, figure) to avoid metadata/alt text
# - All interactive elements (buttons, forms, inputs)
# - Common ad/share/social/comment elements
#
JUNK_SELECTORS = [
    # Structure elements
    "head", "script", "style", "nav", "header", "footer", 
    "iframe", "noscript", "svg", "link", "meta",
    
    # Images and media (to avoid alt text/metadata in content)
    "img", "picture", "figure", "figcaption",
    
    # Interactive elements
    "button", "form", "input", "textarea",
    
    # Common class patterns
    ".header", ".footer", ".sidebar", ".nav", ".menu",
    ".advertisement", ".ad", ".ads", ".sponsor",
    ".share", ".social", ".comments", ".related",
    
    # ID patterns
    "#header", "#footer", "#sidebar", "#nav",
    
    # Attribute patterns
    "[id*='google']", "[class*='google']",
    "[class*='share']", "[class*='social']",
    "[class*='comment']", "[class*='related']",
    "[class*='ad-']", "[id*='ad-']",
    "[class*='newsletter']", "[class*='subscribe']",
    
    # ADD YOUR CUSTOM SELECTORS BELOW:
    # ".your-custom-class",
    # "#your-custom-id",
]

# ------------------------------
# URL EXCLUSION PATTERNS  
# ------------------------------
# Regex patterns to HARD EXCLUDE from crawling (never visit, never add to queue)
# These URLs waste resources and should never be crawled
#
# NOTE: Section pages like /news, /politics are now ALLOWED for discovery
# The crawler will visit them to find article links, but won't mark them as articles
#
# Examples of what to add:
# - r"/some-specific-path/",
# - r"\?param=value",
# - r"\.pdf$",
#
URL_EXCLUDE_PATTERNS = [
    # Share links (social media share buttons) 
    r"\?share=",            # ?share=twitter, ?share=facebook, etc.
    r"[&#]share=",          # &share= or #share=
    
    # Image URLs (thumbnails with dimensions)
    r"/\d+-\d+x\d+/",      # /20-300x225/
    r"\d+x\d+\.(jpg|jpeg|png|gif|webp)",  # image-300x200.jpg
    
    # Comment links
    r"#respond",
    r"#comment",
    
    # Pagination
    r"/page/\d+",
    
    # Author/writer pages
    r"/author/",
    r"/writers?/",
    
    # ADD YOUR CUSTOM PATTERNS BELOW:
    # r"/exclude-this-path/",
]


# ------------------------------
# LLM CLEANING & SUMMARIZATION
# ------------------------------
API_URL = "Your llm api url"
MODEL_NAME = "model name"
API_KEY = "empty"

CLEANING_PROMPT = """
You are a text extractor. Extract ONLY the article text from the HTML.

CRITICAL RULES:
1. DO NOT add any commentary, preambles, or explanations
2. DO NOT say "Here's the cleaned text" or "I've removed..."
3. DO NOT add "TAGS:" or tag sections
4. DO NOT add anything that wasn't in the original article
5. Output ONLY the article text exactly as written

KEEP:
- Article title/heading
- Article paragraphs (original wording)
- Subheadings
- Lists in the article

REMOVE:
- Share buttons
- Navigation
- Footer/header
- Ads
- Related articles
- Comments

OUTPUT:
Start with the title, then the article text.
Nothing else. No commentary. No tags.
"""

SUMMARY_PROMPT = """
Summarize this article in 2–3 sentences. Focus on the main idea only.
"""

def chunk_text(text, max_len=8000):
    """
    Split large HTML pages so GPT-OSS never rejects request size.
    """
    chunks = []
    start = 0
    length = len(text)

    while start < length:
        end = start + max_len
        chunks.append(text[start:end])
        start = end

    return chunks


def html_preclean(html: str) -> str:
    """Strip script/style/comments so GPT-OSS does not choke."""
    html = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    html = re.sub(r"<style[\s\S]*?</style>", " ", html, flags=re.I)
    html = re.sub(r"<!--.*?-->", " ", html, flags=re.S)
    html = re.sub(r"\s+", " ", html)
    return html.strip()


def clean_llm_artifacts(text: str) -> str:
    """
    Remove LLM commentary, preambles, and artifacts from cleaned text.
    LLMs often add their own commentary - this removes it.
    """
    # Remove common LLM preambles
    preambles = [
        r"^.*?Here'?s? the cleaned (?:text|article|content).*?\n",
        r"^.*?I'?ve (?:cleaned|removed|extracted).*?\n",
        r"^.*?adhering to (?:your )?specifications.*?\n",
        r"^.*?As requested.*?\n",
        r"^.*?Following (?:your )?instructions.*?\n",
        r"^Read the .*?\n",
    ]
    
    for pattern in preambles:
        text = re.sub(pattern, "", text, flags=re.I | re.M)
    
    # Remove TAGS section at end (LLM often adds this)
    text = re.sub(r"\n\s*TAGS?:\s*.*$", "", text, flags=re.I | re.S)
    
    # Remove other common postambles
    postambles = [
        r"\n\s*I hope this helps.*$",
        r"\n\s*Let me know if.*$",
        r"\n\s*Please note.*$",
        r"\n\s*Note:.*$",
    ]
    
    for pattern in postambles:
        text = re.sub(pattern, "", text, flags=re.I | re.S)
    
    return text.strip()


def gpt_clean(html: str) -> str:
    """
    Use Gemma LLM to clean article content from HTML.
    """
    try:
        # STEP 1: Extract ONLY the article content area first
        article_html = extract_article_content(html)
        print(f"   [EXTRACT] Reduced HTML from {len(html)} to {len(article_html)} chars")
        
        # STEP 2: Pre-clean the extracted article HTML
        clean_input = html_preclean(article_html)
        chunks = chunk_text(clean_input, max_len=8000)
        outputs = []

        for idx, chunk in enumerate(chunks):
            print(f"   [GEMMA CLEAN] Chunk {idx+1}/{len(chunks)}")

            payload = {
                "model": MODEL_NAME,
                "messages": [
                    {"role": "system", "content": CLEANING_PROMPT},
                    {"role": "user", "content": chunk}
                ],
                "temperature": 0.0,
                "max_tokens": 4096
            }

            r = requests.post(
                API_URL,
                json=payload,
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=200
            )

            if r.status_code != 200:
                print("      ⚠️ Gemma chunk failed — fallback")
                outputs.append(strip_html_basic(chunk))
                continue

            content = (
                r.json()
                  .get("choices", [{}])[0]
                  .get("message", {})
                  .get("content", "")
                  .strip()
            )

            if not content:
                print("      ⚠️ Gemma returned empty — fallback")
                outputs.append(strip_html_basic(chunk))
            else:
                outputs.append(content)

            time.sleep(0.15)

        result = "\n".join(outputs)
        
        # STEP 3: Remove LLM commentary and artifacts
        result = clean_llm_artifacts(result)
        
        # STEP 4: Final safety check - strip any remaining HTML tags
        result = re.sub(r"<[^>]+>", "", result)
        result = re.sub(r"\s+", " ", result)
        
        return result.strip()

    except Exception as e:
        print("⚠️ Gemma cleaning FAILED:", e)
        return strip_html_basic(html)

def extract_article_content(html: str) -> str:
    """
    Aggressively extract ONLY the article content area from full HTML.
    Removes head, scripts, nav, footer, ads, etc. BEFORE text extraction.
    """
    try:
        tree = HTMLParser(html)
        
        # STEP 1: Remove junk using configurable selector list
        for selector in JUNK_SELECTORS:
            for elem in tree.css(selector):
                elem.decompose()
        
        # STEP 2: Try to find main article content
        article = None
        
        # Try various article content selectors (in order of preference)
        selectors = [
            "article",
            "[class*='article-content']",
            "[class*='post-content']",
            "[class*='entry-content']",
            "[class*='story-content']",
            "[id*='article-content']",
            "[id*='post-content']",
            ".article-body",
            ".post-body",
            "main",
            "[role='main']",
            "#main-content",
            ".content"
        ]
        
        for selector in selectors:
            article = tree.css_first(selector)
            if article:
                print(f"   [FOUND] Article container: {selector}")
                break
        
        # STEP 3: If no article found, use body
        if not article:
            article = tree.css_first("body")
            if article:
                print(f"   [FALLBACK] Using <body> as article container")
        
        if not article:
            print(f"   [ERROR] No article container found at all!")
            return strip_html_basic(html)
        
        # Get HTML of just the article area
        article_html = article.html
        
        # STEP 4: Final cleaning of article HTML
        article_html = re.sub(r"<script[\s\S]*?</script>", "", article_html, flags=re.I)
        article_html = re.sub(r"<style[\s\S]*?</style>", "", article_html, flags=re.I)
        article_html = re.sub(r"<!--.*?-->", "", article_html, flags=re.S)
        
        print(f"   [ARTICLE HTML] Length: {len(article_html)} chars")
        return article_html.strip()
        
    except Exception as e:
        print(f"   [ERROR] extract_article_content failed: {e}")
        return strip_html_basic(html)


def gpt_summary(text: str) -> str:
    """
    Generate a 2-3 sentence summary using Gemma.
    Optional - returns empty string on failure.
    """
    try:
        # Limit input to first 4000 chars for summary
        text_sample = text[:4000]
        
        payload = {
            "model": MODEL_NAME,
            "messages": [
                {"role": "system", "content": SUMMARY_PROMPT},
                {"role": "user", "content": text_sample}
            ],
            "temperature": 0.0,
            "max_tokens": 512
        }

        r = requests.post(
            API_URL,
            json=payload,
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=200
        )

        if r.status_code != 200:
            print("      ⚠️ Summary failed - no Gemma")
            return ""

        result = (
            r.json()
              .get("choices", [{}])[0]
              .get("message", {})
              .get("content", "")
              .strip()
        )

        return result or ""

    except Exception as e:
        print(f"      ⚠️ Summary error: {e}")
        return ""


def manual_clean(html: str) -> str:
    """
    Manual HTML cleaning using selectolax - NO LLM.
    More reliable but less sophisticated than LLM cleaning.
    Extracts ONLY text from article elements.
    """
    try:
        print(f"   [MANUAL CLEAN] Starting...")
        
        # STEP 1: Extract article content area first
        article_html = extract_article_content(html)
        
        # STEP 2: Parse with selectolax
        tree = HTMLParser(article_html)
        
        # STEP 3: Remove all unwanted elements (already done in extract_article_content via JUNK_SELECTORS)
        
        # STEP 4: Extract text from article elements only
        parts = []
        
        # Get title/h1
        h1 = tree.css_first("h1")
        if h1:
            title = h1.text(strip=True)
            if title:
                parts.append(title)
                parts.append("")  # blank line after title
        
        # Get article content
        for selector in ["p", "h2", "h3", "h4", "li", "blockquote"]:
            for elem in tree.css(selector):
                text = elem.text(strip=True)
                
                # Filter out junk text
                if len(text) < 3:
                    continue
                
                # Skip if looks like navigation/footer/ads
                lower = text.lower()
                skip_patterns = [
                    "click to share", "share on", "tweet", "share this",
                    "subscribe", "newsletter", "follow us", "sign up",
                    "read more", "continue reading", "related:", "tags:",
                    "posted in", "filed under", "advertisement", "sponsored",
                    "comment", "leave a comment", "view all posts",
                    "copyright", "all rights reserved",
                    "privacy policy", "terms of use", "cookie policy"
                ]
                
                if any(pattern in lower for pattern in skip_patterns):
                    continue
                
                # Skip if it's just a link with no substance
                if text.startswith("http") or text.startswith("www"):
                    continue
                
                parts.append(text)
        
        result = "\n\n".join(parts)
        
        # STEP 5: Final cleanup
        result = re.sub(r"\s+", " ", result)  # normalize spaces
        result = re.sub(r"\n\s*\n\s*\n+", "\n\n", result)  # max 2 newlines
        
        print(f"   [MANUAL CLEAN] Output: {len(result)} chars")
        return result.strip()
        
    except Exception as e:
        print(f"⚠️ Manual cleaning FAILED: {e}")
        return strip_html_basic(html)


def strip_html_basic(html: str) -> str:
    """Fallback text extraction when main cleaning fails."""
    try:
        # First remove all scripts, styles, head, images
        html = re.sub(r"<head[\s\S]*?</head>", "", html, flags=re.I)
        html = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.I)
        html = re.sub(r"<style[\s\S]*?</style>", "", html, flags=re.I)
        html = re.sub(r"<img[\s\S]*?>", "", html, flags=re.I)
        html = re.sub(r"<!--.*?-->", "", html, flags=re.S)
        
        tree = HTMLParser(html)
        parts = []
        for node in tree.css("p, li, h1, h2, h3, h4"):
            t = node.text(strip=True)
            if len(t) > 10:  # Longer threshold to skip junk
                parts.append(t)
        return "\n\n".join(parts)
    except:
        return ""


# ------------------------------
# Metadata Extractor
# ------------------------------

def extract_metadata(html: str):
    tree = HTMLParser(html)

    # TITLE
    title = ""
    og = tree.css_first("meta[property='og:title']")
    if og:
        title = og.attributes.get("content", "")

    if not title:
        h1 = tree.css_first("h1")
        if h1:
            title = h1.text(strip=True)

    if not title:
        title = "Untitled"

    # AUTHOR
    author = ""

    # Look for JSON-LD author
    for n in tree.css("script[type='application/ld+json']"):
        try:
            data = json.loads(n.text(strip=True))
            if isinstance(data, dict) and "author" in data:
                if isinstance(data["author"], list):
                    author = ", ".join(
                        a.get("name", "") for a in data["author"] if "name" in a
                    )
                else:
                    author = data["author"].get("name", "")
        except:
            pass

    if not author:
        meta_author = tree.css_first("meta[name='author']")
        if meta_author:
            author = meta_author.attributes.get("content", "")

    if not author:
        by = tree.css_first(".byline, .author, .post-author")
        if by:
            author = by.text(strip=True).replace("By ", "")

    # DATE
    published = ""

    meta_time = tree.css_first("meta[property='article:published_time']")
    if meta_time:
        published = meta_time.attributes.get("content", "")

    if published:
        try:
            return title, author, datetime.fromisoformat(published)
        except:
            pass

    # If missing → use today
    return title, author, datetime.utcnow()


# ------------------------------
# URL Discovery (universal)
# ------------------------------

def extract_article_links(html: str, base_url: str):
    """
    Handles all broken discovery cases:
    - card links
    - JS-inserted anchors
    - Hindi/Marathi unicode URLs
    - '/politics/<slug>'
    - Medium redirects
    """
    tree = HTMLParser(html)
    links = set()

    for a in tree.css("a"):
        href = a.attributes.get("href")
        if not href:
            continue

        # Resolve relative → absolute
        url = urljoin(base_url, href)

        # Filter non-article patterns
        if any(x in url.lower() for x in [
            "login", "signup", "privacy", "terms",
            "contact", "about", ".jpg", ".png", ".gif"
        ]):
            continue

        # Accept only same-site links
        if urlparse(url).netloc == urlparse(base_url).netloc:
            links.add(url)

    return list(links)


# ------------------------------
# DATABASE UPSERT
# ------------------------------

def upsert_post(db, blog_id, url, title, text, html, author, tags, published, summary): #replace with your columns
    """
    Your logic to update in the database
    """
    db.commit()
    return row[0] if row else None


# ------------------------------
# MAIN ARTICLE CRAWLER
# ------------------------------

def crawl_article(url: str):
    """
    Crawl a single article using plain requests (no Playwright).
    Returns a simple object with html and markdown fields.
    """
    try:
        # More complete headers to avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
        
        response = requests.get(url, timeout=15, headers=headers, allow_redirects=True)
        
        if response.status_code != 200:
            print(f"   ⚠️ HTTP {response.status_code}")
            return None
        
        html = response.text
        
        # Extract text using our basic method
        markdown = strip_html_basic(html)
        
        # Return object that mimics AsyncWebCrawler result
        class CrawlResult:
            def __init__(self, html, markdown):
                self.html = html
                self.markdown = markdown
                self.success = True
        
        return CrawlResult(html, markdown)
        
    except Exception as e:
        print(f"   ⚠️ Crawl error: {e}")
        return None


def discover_all_links(start_url: str, max_pages: int = 500, max_articles: int = 10):
    """
    BFS crawler to explore the entire domain, collecting every article link.
    Uses plain requests (no Playwright) for fast discovery.
    SMART FILTERING: Excludes category/archive pages, only gets real articles.
    """
    visited = set()
    queue = [start_url]
    article_urls = set()

    domain = urlparse(start_url).netloc

    while queue and len(visited) < max_pages and len(article_urls) < max_articles:
        url = queue.pop(0)
        if url in visited:
            continue

        visited.add(url)
        print(f"[DISCOVERY] Visiting: {url}")

        try:
            # Use plain requests - no Playwright needed for discovery
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            if response.status_code != 200:
                continue

            html = response.text
            links = extract_article_links(html, url)

            for link in links:
                # Only stay on the same domain
                if urlparse(link).netloc != domain:
                    continue

                # =====================================================
                # SMART ARTICLE DETECTION - Two-stage filtering
                # =====================================================
                
                # STAGE 1: HARD EXCLUSIONS - Never crawl these, never add to queue
                # These are obvious non-article pages that waste resources
                hard_exclude_patterns = [
                    r"\?share=",            # Share links
                    r"[&#]share=",          # Share parameters
                    r"/\d+-\d+x\d+/",      # Image dimensions
                    r"\d+x\d+\.(jpg|jpeg|png|gif|webp)",  # Thumbnail files
                    r"#respond",            # Comment links
                    r"#comment",
                    r"/page/\d+",           # Pagination
                    r"/author/",            # Author pages
                    r"/writers?/",          # Writer pages
                ]
                
                if any(re.search(pattern, link, re.I) for pattern in hard_exclude_patterns):
                    continue
                
                # STAGE 2: SOFT EXCLUSIONS - Can visit for discovery, but don't mark as articles
                # These are section/category pages that might contain article links
                soft_exclude_patterns = [
                    r"/category/?$",
                    r"/categories/?$",
                    r"/tag/?$",
                    r"/tags/?$",
                    r"/topics?/?$",
                    r"/archive/?$",
                    r"/archives/?$",
                    r"/\d{4}/?$",           # Just year: /2023/
                    r"/\d{4}/\d{2}/?$",     # Just year/month: /2023/08/
                    r"/hub/?$",
                    r"/all-articles/?$",
                    r"/news/?$",            # Section page, not article
                    r"/world-news/?$",
                    r"/politics/?$",
                    r"/opinion/?$",
                    r"/sports/?$",
                    r"/tech/?$",
                    r"/business/?$",
                    r"/story/?$",           # Story section, not article
                ]
                
                is_section_page = any(re.search(pattern, link, re.I) for pattern in soft_exclude_patterns)
                
                # Add to queue for further discovery (even if it's a section page)
                if link not in visited:
                    queue.append(link)
                
                # STAGE 3: ARTICLE DETECTION - Only mark as article if it passes these tests
                if not is_section_page:
                    is_article = False
                    
                    # Pattern 1: Has /article/ in URL (common for news sites)
                    if "/article/" in link:
                        is_article = True
                        print(f"   ✓ Article pattern: /article/ in URL")
                    
                    # Pattern 2: News/Story with full date path
                    # /news/2024/01/26/title-slug or /story/2024/01/26/title-slug
                    elif re.search(r"/(news|story)/20\d{2}/\d{2}/\d{2}/[\w-]+", link):
                        is_article = True
                        print(f"   ✓ Article pattern: /news or /story with full date")
                    
                    # Pattern 3: Date-based URL with day (full article URL)
                    # /2024/01/26/title-slug/ or /2024/01/26/title-slug
                    elif re.search(r"/20\d{2}/\d{2}/\d{2}/[\w-]+", link):
                        is_article = True
                        print(f"   ✓ Article pattern: Full date path")
                    
                    # Pattern 4: WordPress-style: /year/month/slug (no day)
                    elif re.search(r"/20\d{2}/\d{2}/[\w-]{10,}", link) and not link.endswith('/'):
                        is_article = True
                        print(f"   ✓ Article pattern: WordPress date style")
                    
                    # Pattern 5: Long unique slug at end (likely article, not category)
                    # Must be at least 20 chars and not end with common category names
                    elif re.search(r"/[\w-]{20,}/?$", link):
                        slug = link.rstrip('/').split('/')[-1]
                        category_words = [
                            'news', 'politics', 'world', 'business', 'sports', 
                            'tech', 'technology', 'science', 'opinion', 'entertainment', 
                            'lifestyle', 'health', 'culture', 'economy', 'society'
                        ]
                        if slug.lower() not in category_words:
                            is_article = True
                            print(f"   ✓ Article pattern: Long slug ({len(slug)} chars)")
                    
                    # Add to article list if it passes filters
                    if is_article and link not in article_urls:
                        article_urls.add(link)
                        print(f"   📰 ARTICLE ADDED: {link}")
                        
                        if len(article_urls) >= max_articles:
                            print(f"[DISCOVERY] ✅ HARD STOP: reached {max_articles} article URLs")
                            break

            time.sleep(random.uniform(1.5, 4.0))
  # Be nice to servers

        except Exception as e:
            print(f"[DISCOVERY] Error: {e}")

    print(f"[DISCOVERY COMPLETE] Found {len(article_urls)} articles")
    return list(article_urls)[:max_articles]


# ------------------------------
# CRAWL SITE
# ------------------------------

async def crawl_site(db, blog, site_url):
    print(f"\n🌐 Crawling site: {site_url}")

    # STEP 1 — FULL SITE DISCOVERY using BFS (plain requests, no Playwright)
    print("🔍 Discovering all links...")
    article_urls = discover_all_links(site_url, max_pages=500, max_articles=10)
    
    print(f"📝 Identified {len(article_urls)} article URLs")

    inserted = 0
    skipped = 0

    # STEP 2 — CRAWL EACH ARTICLE
    for u in article_urls:
        if inserted >= 10:
            print("[CRAWL] ✅ HARD STOP: inserted 10 posts")
            break
            
        print(f"\n➡️ Crawling article: {u}")
        res = crawl_article(u)

        if not res:
            print(f"   ❌ SKIP: crawl_article returned None (request failed)")
            skipped += 1
            continue
            
        if not res.success:
            print(f"   ❌ SKIP: crawl_article returned success=False")
            skipped += 1
            continue

        html = res.html or ""
        raw_text = res.markdown or strip_html_basic(html)

        print(f"   [DEBUG] HTML length: {len(html)} chars")
        print(f"   [DEBUG] Raw text length: {len(raw_text)} chars")

        if len(raw_text.strip()) < 50:
            print(f"   ❌ SKIP: Content too short ({len(raw_text.strip())} chars < 50)")
            print(f"   [PREVIEW] First 200 chars: {raw_text[:200]}")
            skipped += 1
            continue

        title, author, published = extract_metadata(html)
        print(f"   [METADATA] Title: {title[:50]}...")
        print(f"   [METADATA] Author: {author}")
        
        # Extract clean text (choose method based on flag)
        if USE_LLM_CLEANING:
            cleaned_text = gpt_clean(html)
        else:
            cleaned_text = manual_clean(html)
        
        print(f"   [CLEAN] Cleaned text length: {len(cleaned_text)} chars")
        
        if len(cleaned_text.strip()) < 50:
            print(f"   ❌ SKIP: Cleaned content too short ({len(cleaned_text.strip())} chars)")
            skipped += 1
            continue
        
        # Also clean the HTML field (remove doctype, head, scripts)
        cleaned_html = extract_article_content(html)
        
        # Generate summary
        summary = gpt_summary(cleaned_text)

        post_id = upsert_post(
            db, blog.id, u,
            title, cleaned_text, cleaned_html,  # both cleaned now
            author, [], published, summary
        )

        if post_id:
            print(f"   ✅ Inserted Post #{post_id}")
            inserted += 1
        else:
            print(f"   ⚠️ SKIP: Post already exists in database (url_canonical conflict)")
            skipped += 1

        time.sleep(0.2)

    print(f"\n📦 Done: Inserted={inserted}, Skipped={skipped}")
    return inserted, skipped


# ------------------------------
# MAIN
# ------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sites-file", default="app/feed/sites.json")
    args = parser.parse_args()

    with open(args.sites_file, "r") as fp:
        sites = json.load(fp)

    db = SessionLocal()

    total = 0
    for site in sites:
        site_url = site["site_url"]
        rss = site.get("rss_url")

        stmt = select(Blog).where(
            (Blog.url == site_url) | (Blog.rss_url == rss)
        )
        blog = db.execute(stmt).scalar()

        if not blog:
            blog = Blog(
                name=urlparse(site_url).netloc,
                url=site_url,
                rss_url=rss,
                language="en"
            )
            db.add(blog)
            db.commit()
            db.refresh(blog)

        new, skipped = asyncio.run(crawl_site(db, blog, site_url))
        total += new

    print(f"\n🚀 Finished. Total new posts: {total}")


if __name__ == "__main__":
    main()
