# Gmail Analyzer

![screenshot](screenshots/tool.png)


This tool will analyze your gmail account to show you statistics of your emails. e.g.

- Total number of emails
- First email received
- Top senders
- Distribution of emails by years

There are many metrics that can be added, feel free to contribute (or open a ticket!).

More information in [this blog post](https://mhasbini.com/blog/introducing-gmail-analyzer.html).


# Installation

```shell
$ git clone https://github.com/0xbsec/gmail_analyzer.git
$ cd gmail_analyzer
$ pipenv install
$ python analyzer.py --help
```

# Usage

```
$ python analyzer.py --help
usage: analyzer.py [-h] [--top TOP] [--user USER] [--query QUERY]
                   [--inactive INACTIVE] [--max-retry-rounds MAX_RETRY_ROUNDS]
                   [--pull-data] [--refresh-data] [--analyze-only]
                   [--export-csv EXPORT_CSV] [--verbose] [--version]

Simple Gmail Analyzer

optional arguments:
  -h, --help            show this help message and exit
  --top TOP             Number of results to show
  --user USER           User ID to fetch data for
  --query QUERY         Gmail search query (e.g., 'label:work after:2023/01/01')
  --inactive INACTIVE   Show senders inactive for more than X days
  --max-retry-rounds MAX_RETRY_ROUNDS
                        Max retry rounds for failed message fetches (0 for unlimited)
  --pull-data           Fetch and cache data, then exit
  --refresh-data        Force refresh cached data, then exit
  --analyze-only        Analyze using cached data only (no API calls)
  --export-csv EXPORT_CSV
                        Export message metadata to CSV at the given path
  --verbose             Verbose output, helpful for debugging
  --version             Display version and exit
```

# Caching & Data Pulls

The analyzer caches message and metadata pickles in `cache/` for 24 hours. Queries
create separate cache files, so cached data stays scoped to each Gmail search.

Examples:

```
$ python analyzer.py --pull-data --query "label:work after:2023/01/01"
$ python analyzer.py --analyze-only --export-csv out/messages.csv
```
