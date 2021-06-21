from datetime import datetime as dt
import scrapy

class RedditSpider(scrapy.Spider):
    name = 'reddit'

    def start_requests(self):
        yield scrapy.Request('http://reddit.com')

    def parse(self, response):
        items = []
        for div in response.css('div.sitetable div.thing'):
            try:
                title = div.css('p.title a::text').extract_first()
                votes_div = div.css('div.score.unvoted')
                votes = votes_div.css('::attr(title)').extract_first()
                votes = votes or votes_div.css('::text').extract_first()

                items.append({'title': title, 'votes': int(votes)})
            except:
                pass

        if len(items) > 0:
            timestamp = response.meta['wayback_machine_time'].timestamp()
            return {'timestamp': timestamp, 'items': items}