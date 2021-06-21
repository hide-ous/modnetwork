import scrapy


class RedditSpider(scrapy.Spider):
    name = 'reddit'

    def start_requests(self):
        yield scrapy.Request('https://www.reddit.com/r/conspiracy/about/moderators')

    def parse(self, response):
        items = []
        for div in response.css('div.moderator-table'):
            for row in div.css('tr'):
                cur_user = dict()
                try:
                    cur_user['name'] = row.css('span.user a::text').extract_first()
                    cur_user['karma'] = row.css('span.user b::text').extract_first()
                    cur_user['since'] = row.css('time::attr(datetime)').extract_first()
                    cur_user['permissions'] = row.css('span.permission-bit::text').extract_first()

                    items.append(cur_user)
                except:
                    pass
        timestamp = response.meta['wayback_machine_time'].timestamp()
        return {'timestamp': timestamp, 'items': items}
