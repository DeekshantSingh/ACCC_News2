import requests
from parsel import Selector
import re
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata

# Configure retries for SSL errors
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)

# Cookies and headers for requests
cookies = {
    '_ga': 'GA1.1.1489870566.1731580940',
    'monsido': '5281731580947515',
    '_ga_S5TGQHQ4G8': 'GS1.1.1731580939.1.1.1731582032.0.0.0',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
}

# List to store data
data_entry = []

def clean_text(text):
    """Remove extra spaces, non-breaking spaces, and unwanted characters."""
    # Normalize text to remove weird characters
    text = unicodedata.normalize("NFKC", text)
    # Replace non-breaking spaces and control characters with regular spaces
    text = re.sub(r'[\u00A0\u200B]+', ' ', text)
    # Remove clutter like "× Close Click to enlarge"
    text = text.replace('×', '').replace('Close Click to enlarge', '')
    # Collapse multiple spaces into one
    text = ' '.join(text.split())
    return text.strip()

def save_to_excel(filename='ACCC_all_news_2.xlsx'):
    """Save extracted data to an Excel file."""
    df = pd.DataFrame(data_entry).fillna("N/A")
    df.to_excel(filename, index=False, engine='openpyxl')
    print(f"Data saved to {filename}")

def find_penalty_sentences(text):
    """Find sentences containing penalty-related keywords."""
    keywords = r'\b(penalty|penalties|fine|fines|fined)\b'
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)
    return [sentence for sentence in sentences if re.search(keywords, sentence, re.IGNORECASE)]

def extract_penalty_amounts(penalty_sentences):
    """Extract penalty amounts from sentences."""
    pattern = r'(S?\$)(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(million|billion|trillion)?'
    penalty_amounts = []
    for sentence in penalty_sentences:
        matches = re.findall(pattern, sentence, re.IGNORECASE)
        for match in matches:
            amount = match[0] + match[1]
            if match[2]:  # Add large unit if present
                amount += f' {match[2]}'
            penalty_amounts.append(amount)
    return penalty_amounts

def format_date(date_str):
    """Format date strings into YYYY-MM-DD."""
    for fmt in ("%d %B %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str

def extract_contact_info(input_text):
    """Extract contact number and email from text."""
    contact_number, email = "N/A", "N/A"

    # Regex for contact numbers
    contact_num_pattern = r'(\d{4} \d{3} \d{3}|\d{4} \d{6}|\d{4} \d{3} \d{4})'
    contact_match = re.search(contact_num_pattern, input_text)
    if contact_match:
        contact_number = contact_match.group()

    # Regex for emails
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    email_match = re.search(email_pattern, input_text)
    if email_match:
        email = email_match.group()

    return contact_number, email

def process_individual_news(final_url, news_heading, news_summary):
    """Fetch details for an individual news article."""
    try:
        response2 = http.get(final_url, headers=headers, cookies=cookies, timeout=10)
        parsed_data2 = Selector(response2.text)

        news_details_contents = parsed_data2.xpath('//div[contains(@class,"field--type-text-long")]//text()').getall()
        if news_details_contents:
            news_details_contents = clean_text(' '.join(news_details_contents))

        find_penalty_s = find_penalty_sentences(news_details_contents)
        extract_penalty_a = "|".join(extract_penalty_amounts(find_penalty_s)) or "N/A"

        Date_of_press_release = parsed_data2.xpath('//div[@class="field__item"]//time//text()').get()
        Date_of_press_release = format_date(Date_of_press_release) if Date_of_press_release else 'N/A'

        Release_number = parsed_data2.xpath('//h3[contains(text(), "Release number")]/following-sibling::div//text()').get()
        Release_number = Release_number.strip() if Release_number else 'N/A'

        Topics = parsed_data2.xpath('//*/div[@class="field__items"]/div/a/text()').getall()
        Topics = '|'.join(Topics).replace('and', '|') if Topics else 'N/A'

        General_enquiries = parsed_data2.xpath(
            '//h3[contains(text(), "General enquiries")]/following-sibling::div//text()').getall()
        General_enquiries = clean_text(' '.join([text.strip() for text in General_enquiries if text.strip()])) if General_enquiries else 'N/A'

        Media_enquiries = parsed_data2.xpath(
            '//h3[contains(text(), "Media enquiries")]/following-sibling::div//text()').getall()
        if Media_enquiries:
            Media_contact_num, Media_email = extract_contact_info(clean_text(' '.join(Media_enquiries)))
        else:
            Media_contact_num, Media_email = 'N/A', 'N/A'

        return {
            "Topics": Topics,
            "Date_release": Date_of_press_release,
            "Release_number": Release_number,
            "news_url": final_url,
            "news_heading": news_heading.strip(),
            "news_summary": news_summary.strip(),
            "penalty_amounts": extract_penalty_a,
            "General_enquiries": General_enquiries,
            "Media_contact_num": Media_contact_num,
            "Media_email": Media_email,
            "news_details_content": news_details_contents
        }

    except requests.exceptions.RequestException as e:
        print(f"Error fetching {final_url}: {e}")
        return None

# Main pagination loop
pagination = 0
while True:
    try:
        response = http.get(
            f'https://www.accc.gov.au/news-centre?items_per_page=100&accc_page_settings_path=%2Fnews-centre&page={pagination}',
            cookies=cookies, headers=headers, timeout=10
        )
        parsed_data = Selector(response.text)
        rows = parsed_data.xpath('//div[@class="view-content"]//div[contains(@class,"card-wrapper")]')

        news_data = []
        for row in rows:
            news_url = row.xpath('.//a[@class="accc-news-card__link row"]//@href').get()
            home_page = 'https://www.accc.gov.au'
            final_url = home_page + news_url
            news_heading = row.xpath('.//a[@class="accc-news-card__link row"]//h2//text()').get()
            if news_heading:
                news_heading = ' '.join(news_heading.split())
            else:
                news_heading = 'N/A'

            news_summary = row.xpath('.//div[contains(@class,"summary")]//text()').get()
            if news_summary:
                news_summary = ' '.join(news_summary.split())
            else:
                news_summary = 'N/A'
            news_data.append((final_url, news_heading, news_summary))

        # Multithreading for fetching individual news details
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(lambda x: process_individual_news(*x), news_data)
            data_entry.extend(filter(None, results))  # Filter out failed results

        next_page = parsed_data.xpath('//li[@class="page-item page-item--last"]').get()
        if next_page:
            pagination += 1
            print(f"Moving to page {pagination}...")
        else:
            break

    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {pagination}: {e}")
        break

# Save the results
save_to_excel()
print("Done")
