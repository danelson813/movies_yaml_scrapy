import scrapy
from scrapy.crawler import CrawlerProcess
from selectorlib import Extractor, Formatter


class Joined(Formatter):
    def format(self, text):
        base = 'https://www.imdb.com'
        if text:
            return base + text
        return None


class Year(Formatter):
    def format(self, text):
        return text.replace("(", '').replace(')', '')


class TestSpider(scrapy.Spider):
    name = "movies"
    custom_settings = {'DOWNLOAD_DELAY': 1}

    formatters = [Joined]
    # form an extractor
    e = Extractor.from_yaml_file('selectors.yml', formatters=formatters)

    def start_requests(self):
        url = "https://www.imdb.com/chart/top/?ref_=nv_mv_250"
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        formatters = [Joined, Year]
        e = Extractor.from_yaml_file('pages.yml', formatters=formatters)
        links = e.extract(response.page_source)
        print(f'The length of links is {len(links)}.')
        for link in links:
            print(f'link is {link}')
            yield scrapy.Request(url=link, callback=self.parse_page)

    def parse_page(self, response):
        formatters = [Joined, Year]
        ex = Extractor.from_yaml_file('movies.yml', formatters=formatters)
        data = ex.extract(response.page_source)
        yield data


if __name__ == '__main__':
    process = CrawlerProcess({
        "USER-AGENT": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        "FEED_FORMAT": "json",
        "FEED_URI": "data.json"
    })
    process.crawl(TestSpider)
    process.start()
