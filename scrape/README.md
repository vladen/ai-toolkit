# Web page scraper tool/daemon on python

Command-line tools to:

- scrape texts from web pages store them JSON/CSV files
- upload scraped texts to Chroma DB
- TODO: query Chat GPT for summaries of scraped texts, store in Chroma DB
- TODO: prepare training set for Chat GPT, based on scraped texts

## Installation

Assuming that you already have `python 3.10+` and `pip` onboard...

```bash
$ pip install --user -U chromadb
$ pip install --user -U nltk
$ python -m nltk.downloader punkt
$ pip install --user -U openai
$ pip install --user -U tiktoken
$ pip install --user -U unstructured
```

Create env variable for `OPENAI_API_KEY` exporting your key.

## Usage

### Scraping web site:

```bash
$ python ./scrape/scrape.py "<url>" <folder>  -f "<filter>" -l 100 -v 0
```

Where:

- `<url>` - required, url of initial web page to scrape;
- `<folder>` - required, path to working folder where scraped data will be saved;
- `-l <number>` or `--limit <number>` - optional, maximum number of web pages to scrape;
- `-f "<filter>"` or `--filter "<filter>"` - optional, regular expression to filter URLs to be scraped;
- `-v <boolean>` or `--verbose <boolean>` - optional, verbose mode, true by default.

#### Notes

If argument `filter` is not provided, only urls having same `schema/domain/port` as the initial url, and having no extension or `.htm`/`.html` extension will be scraped.

Existing scraping session continues, if scraping is started again with same `url/folder` args.
This tool only scrapes new pages, without checking if already scraped were updated.

### Uploading data to Chroma DB:

```bash
$ python ./scrape/upload.py <folder> "<chroma>" -l <number> -v 1
```

Where:

- `<folder>` - required, path to folder with scraped files;
- `"<chroma>"` - required, path to Chroma DB folder;
- `-l <number>` or `--limit <number>` - optional, maximum number of documents to upload.

### Querying Chroma DB:

```bash
$ python ./scrape/query.py <folder> "<text>" -l <number> -v 1
```

Where:

- `<folder>` - path to Chroma DB folder;
- `"<text>"` - text to search in Chroma DB;
- `-l <number>` or `--limit <number>` - optional, maximum number of documents;

---

## Running as MacOS daemon

To run scrape script as a daemon (in background, even when your laptop sleeps),
update `scrape/local.scrape.project.plist`

- copy template file and replace `.project.` part in the target file name with the name of your project,
- open target file and replace `{project}` and `{user}` placeholders and with the correct values,
- use same values for same placeholders in examples below,
- copy the file to the daemon folder,
- run new daemon.

### Copying file to the daemon folder

```bash
$ cp scripts/scrape/local.scrape.name.plist /Library/LaunchDaemons
```

### Running the daemon

```bash
$ sudo launchctl start local.scrape.helpx
```

### Checking daemon status

```bash
$ sudo launchctl list | grep local.scrape.{name}
```

### Checking stdout and stderr logs

```bash
$ tail -f /Users/{user}/Projects/private/ML/datasets/adobe/helpx/stdout.log
$ tail -f /Users/{user}/Projects/private/ML/datasets/adobe/helpx/stderr.log
```

### Stopping and reloading the daemon

Reloading is needed of `.plist` file was updated.

```bash
$ sudo launchctl stop local.scrape.helpx
$ sudo launchctl unload /Library/LaunchDaemons/local.scrape.{project}.plist
$ sudo launchctl load /Library/LaunchDaemons/local.scrape.{project}.plist
```

### Checking number of files in target folder

```bash
$ cd /Users/{user}/projects/datasets/{project}
$ find . -type f | wc -l
```
