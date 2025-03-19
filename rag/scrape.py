import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import json

visited = set()

def crawl(url, base_url):
    if url in visited:
        return
    visited.add(url)

    response = requests.get(url)
    if response.status_code != 200:
        return

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract text from page
    print(f"Scraping: {url}")
    print(soup.get_text())

    # Find all internal links and crawl them
    for a_tag in soup.find_all("a", href=True):
        link = urljoin(base_url, a_tag["href"])  # Convert relative URLs to absolute
        if base_url in link:  # Ensure it stays within the site
            crawl(link, base_url)
    return soup.get_text()

def run_crawler_faq_site():
    base_website = "https://www.nvidia.com/gtc/faq/"
    text = crawl(base_website, base_website)
    qa_pairs = re.split(r'\n{2,}', text.strip())
    data = []
    for i in range(0, len(qa_pairs) - 1, 2):
        question = qa_pairs[i].strip()
        answer = qa_pairs[i + 1].strip()
        category = "General"
        if "register" in question.lower() or "cost" in question.lower() or "discount" in question.lower():
            category = "Registration"
        data.append({"category": category, "question": question, "answer": answer})
    return data