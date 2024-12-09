import os
import re
from typing import Iterable, Union

import pandas as pd
import scrapy
from scrapy import Request, Spider
from scrapy.cmdline import execute
from twisted.internet.defer import Deferred


class ZppaOrgZmDataSpider(scrapy.Spider):
    name = "zppa_org_zm_data"

    def __init__(self):
        super().__init__()
        self.data_list = []

    def start_requests(self):
        cookies = {
            'COOKIE_SUPPORT': 'true',
            'GUEST_LANGUAGE_ID': 'en_ZM',
            '_ga': 'GA1.3.1706871265.1732865393',
            'BIGipServer~PROD~zppPROD-portal_pool': '185906442.20480.0000',
            '_gid': 'GA1.3.88843213.1733199236',
            'JSESSIONID': '766F7954B0E3AD246E2E0CDAB93C5234',
            '_ga_73MBQP2X8Z': 'GS1.3.1733208569.3.1.1733209633.0.0.0',
            'LFR_SESSION_STATE_20159': '1733209635433',
        }

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            # 'Cookie': 'COOKIE_SUPPORT=true; GUEST_LANGUAGE_ID=en_ZM; _ga=GA1.3.1706871265.1732865393; BIGipServer~PROD~zppPROD-portal_pool=185906442.20480.0000; _gid=GA1.3.88843213.1733199236; JSESSIONID=766F7954B0E3AD246E2E0CDAB93C5234; _ga_73MBQP2X8Z=GS1.3.1733208569.3.1.1733209633.0.0.0; LFR_SESSION_STATE_20159=1733209635433',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        yield scrapy.Request(
            url="https://www.zppa.org.zm/list-of-debarred-firms",
            headers=headers,
            cookies=cookies
        )

    def parse(self, response, **kwargs):
        article = response.xpath('//div[@class="journal-content-article"]/p')
        for data in article:
            data_dict = {}
            text = ' '.join(data.xpath('./strong//text()').getall())
            text = ' '.join(text.split())
            print(text)
            link = data.xpath('.//a/@href').get()
            # Regular expression pattern
            pattern = r"\d*\.\s*(?P<company_name>[\w\s&.,]+?)\s*(-\s*)?(?P<years>\b[\w]+\s*\([\w\s]+\)\s*years?)\s*suspension effective\s*(?P<date>[\w\s,]+)(?=\s*\()"

            # Search the string
            match = re.search(pattern, text)
            if match:
                company_name = match.group("company_name")
                years = match.group("years")
                date = match.group("date")

                data_dict['company_name'] = company_name.strip()
                data_dict['years'] = years.strip()
                data_dict['date'] = date.strip()
                data_dict['circular_link'] = link
                # Print the extracted information
            else:
                # Regex patterns
                company_pattern = r"\d*\.(\d*\.?\s*[\w\s&.,]+Limited)"
                person_pattern = r"Mr\.\s[\w\s\.]+"

                # Find matches
                companies = re.findall(company_pattern, text)
                persons = re.findall(person_pattern, text)

                data_dict['company_name'] = ' | '.join(companies).strip()
                data_dict['person_name'] = ' | '.join((' | '.join(persons)).split('and')).strip()
                data_dict['circular_link'] = link
            self.data_list.append(data_dict)

    def close(self, spider: Spider, reason: str):
        df = pd.DataFrame(self.data_list)
        df = df.replace(r'^\s*$', None, regex=True)
        df.dropna(how='all', inplace=True)
        df.fillna('N/A', inplace=True)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        df.insert(0, 'id', range(1, len(df) + 1))
        df.insert(1, 'url', 'https://www.zppa.org.zm/list-of-debarred-firms')
        os.makedirs('../output', exist_ok=True)
        df.to_excel('../output/zppa_org_zm.xlsx', index=False)


if __name__ == '__main__':
    execute(f'scrapy crawl {ZppaOrgZmDataSpider.name}'.split())
