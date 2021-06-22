# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from datetime import timezone

import pandas as pd
from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from scrapy_wayback_machine import WaybackMachineMiddleware


class ModScraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ModScraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class SubsampledWaybackMachineMiddleware(WaybackMachineMiddleware):
    def __init__(self, crawler):
        super(SubsampledWaybackMachineMiddleware, self).__init__(crawler=crawler)
        frequency = crawler.settings.get('WAYBACK_MACHINE_FEQUENCY')
        self.set_frequency(frequency)
        self.min_snapshots = crawler.settings.get('WAYBACK_MACHINE_MIN_SNAPSHOTS')

    def set_frequency(self, frequency):
        min_date, max_date = self.time_range
        self.frequency = frequency
        self.date_range = pd.date_range(start=min_date * (10 ** 9), end=max_date * (10 ** 9),
                                        freq=self.frequency, tz='UTC').astype(int) / (10 ** 9)

    def filter_snapshots(self, snapshots):
        snapshots = super(SubsampledWaybackMachineMiddleware, self).filter_snapshots(snapshots=snapshots)
        snapshot_dates = pd.DataFrame(map(lambda snapshot: snapshot['datetime'].timestamp(), snapshots))
        snapshot_dates.columns = ['snapshot_date']

        def closest_date_in_range(snapshot_date):
            return min(self.date_range, key=lambda x: abs(x - snapshot_date))

        snapshot_dates['closest_date'] = snapshot_dates.snapshot_date.apply(closest_date_in_range)
        snapshot_dates.drop_duplicates(subset=['closest_date'], keep='first', inplace=True)
        keep_snapshots = snapshot_dates.index.tolist()
        snapshots = [snapshots[i] for i in keep_snapshots]

        if len(snapshots) < self.min_snapshots:
            return []
        return snapshots
