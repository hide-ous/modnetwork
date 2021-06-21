# modnetwork
how moderators move across reddit

## run
```shell
cd mod_scraper
scrapy crawl mods -o snapshots.jsonlines -a subreddit_file_location=subreddits.json.zst
```