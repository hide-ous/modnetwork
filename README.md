# modnetwork
Use the waybackmachine to extract the roster of subreddit moderators over time. 

## install
```shell
git clone git@github.com:hide-ous/modnetwork.git
cd modnetwork
python -m venv modnet
source modnet/bin/activate
pip install -r requirements.txt
```

## run
```shell
cd mod_scraper
scrapy crawl mods -o snapshots.jsonlines -a subreddit_file_location=subreddits.json.zst
```