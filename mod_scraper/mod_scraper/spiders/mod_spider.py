import itertools
import json
import os

import requests as requests
import scrapy
import tqdm as tqdm
import zstandard as zstd
import pandas as pd

SUBREDDIT_LIST_URL = 'https://files.pushshift.io/reddit/subreddits/reddit_subreddits.ndjson.zst'


def download(url, store_path):
    response = requests.get(url, stream=True)

    chunk_size = 1024 ** 2
    total_size = int(response.headers['Content-Length'])

    with open(store_path, "wb+") as out_file:
        for chunk in tqdm.tqdm(response.iter_content(chunk_size=chunk_size),
                               "downloading " + url + " to " + store_path,
                               int(total_size / chunk_size)):
            out_file.write(chunk)


def decompress_zst_line(path_in):
    with open(path_in, 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            previous_line = ""
            while True:
                chunk = reader.read(2 ** 24)
                if not chunk:
                    break

                string_data = chunk.decode('utf-8')
                lines = string_data.split("\n")
                for i, line in enumerate(lines[:-1]):
                    if i == 0:
                        line = previous_line + line
                    yield line
                previous_line = lines[-1]


def get_subreddits(subreddit_lst_path):
    if not os.path.exists(subreddit_lst_path):
        download(SUBREDDIT_LIST_URL, subreddit_lst_path)
    subreddits = pd.DataFrame(map(json.loads, itertools.islice(decompress_zst_line(subreddit_lst_path), 1000)))
    subreddits = subreddits[['created_utc', 'display_name', 'subscribers', ]]
    subreddits = subreddits.sort_values('subscribers', ascending=False)
    return subreddits.display_name.values.tolist()


class RedditSpider(scrapy.Spider):
    name = 'mods'

    def start_requests(self):
        subreddits = get_subreddits('subreddits.ndjson.zst')

        for subreddit in subreddits:
            yield scrapy.Request('https://www.reddit.com/r/{}/about/moderators'.format(subreddit),
                                 meta={'subreddit': subreddit})

    def parse(self, response):
        items = []
        for div in response.css('div.moderator-table'):
            for row in div.css('tr'):
                cur_user = dict()
                try:
                    cur_user['name'] = row.css('span.user a::text').extract_first()
                    cur_user['karma'] = row.css('span.user b::text').extract_first()
                    cur_user['since'] = row.css('time::attr(datetime)').extract_first()
                    cur_user['permissions'] = row.css('div.permission-summary span::text').extract_first()

                    items.append(cur_user)
                except:
                    pass

        timestamp = response.meta['wayback_machine_time'].timestamp()
        url = response.meta['wayback_machine_url']
        subreddit = response.meta['subreddit']
        return {'timestamp': timestamp, 'url': url, 'items': items, 'subreddit': subreddit}
