# Data Extraction for WA Law Explorer
In this document, I will attempt to explain how I extracted data from Court Listener's [bulk legal data files](https://www.courtlistener.com/help/api/bulk-data/) for my [WA Law Explorer](http://170.205.38.113/cases/) project.

## The project
New to the State of Washington, I decided that I wanted to build a tool to intuitively explore its statutes and case law. My initial MVP was simply to (try to) download all Washington opinion texts available through Court Listener, and a copy of the Revised Code of Washington (RCW), and then parse the opinion text for citations to the RCW from the opinions to build out a database linking statutes to cases. 

## The RCW download
This was easy enough. With the assistance of AI vibe coding, I created [rcw_spider.py](https://github.com/orgullomoore/WA-expl/blob/main/rcw_spider.py), which crawled the Legislature's website to get the [~51.6k code sections](https://huggingface.co/datasets/orgullomoore/RCW) and put those in a sqlite3 file. I later converted this to a parquet file and uploaded it to Hugging Face as a dataset.

## The Court Listener bulk data files
This was the hard part. Court Listener makes its "Bulk Legal Data" available for anyone to download [here](https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/). The files are exports from their database tables, which they run every month or so. The process that Court Listener uses to create these files is disclosed [here](https://github.com/freelawproject/courtlistener/blob/main/scripts/make_bulk_data.sh). I provided the shell script found there to Claude and asked what it did, and it said:
>The script exports an entire legal database to compressed CSV files and uploads them to Amazon S3 for public access as bulk data downloads. For each table, it runs a PostgreSQL COPY command to export data as CSV, pipes the output through bzip2 compression, and streams directly to S3 without storing large files locally.

I wanted only case law specific to Washington, but there is no way to download only Washington opinions. You have to download everything and then pick out what you're looking for. In addition, it's not all in one place. As the Court Listener documentation says, they have separate files for (as relevant to my project):

- Courts - A "dump of court table and contains metadata about the courts we have in our system. Because nearly every data type happens in a court, you'll probably need this table to import any other data type below. We suggest importing it first." The filename looks something like "courts-2025-12-02.csv.bz2" and is 78.9 kB.
- Dockets - A collection of "high-level case information like the docket number, case name, etc." with "many millions of rows" that "should be imported before the opinions data[.]" The file looks something like "dockets-2025-12-02.csv.bz2" and is 4.5 GB.
- Opinion Clusters - A mapping of which majority opinions, concurrences, and dissents belong to single "clusters," i.e., the court's disposition of a particular case. The file looks something like "opinion-clusters-2025-12-02.csv.bz2" and is 2.3 GB.
- Opinions - The full text of the opinions. The file looks something like "opinions-2025-12-02.csv.bz2" and is 49.9 GB. 
- Citations Map - A "narrow table that indicates which opinion cited which and how deeply."
- Parentheticals - A collection of explanatory parentheticals, which are useful for determining what proposition a given case is cited for e.g., the part in parenthesis in: "*Merrion v. Jicarilla Apache Tribe*, 455 U.S. 130, 137, 102 S.Ct. 894, 71 L.Ed.2d 21 (1982) **(holding that only in Indian country may tribes exercise powers over nonmembers)**."

If you just download the opinions file, you will not have any information useful to a human. You could, theoretically, unzip the 50 GB compressed file and end up with a gigantic (probably 500+ GB) text file, which you could, theoretically, load into a text editor efficient enough to handle it on a computer big enough to hold it. That's obviously not feasible. The solution, AI told me, is to use streaming. Quoth Claude: 
>Streaming refers to reading and processing the file incrementally rather than loading the entire decompressed content into memory at once. This is crucial for large files. The key benefit of streaming is memory efficiency - you only keep small portions of the file in memory at any time, making it possible to process files larger than your available RAM.

With Claude's assistance, I created the following simple script to read the first twenty-or-so lines of the opinions-2025-12-02.csv.bz2 file:

```python
import bz2
import csv
import io

with bz2.BZ2File('opinions-2025-12-02.csv.bz2', 'r') as f:
    text_file = io.TextIOWrapper(f, encoding='utf-8')
    csv_reader = csv.reader(text_file)
    count = 0
	for row in csv_reader:
        if count < 20:
			print(row)
			count += 1
```

The output was:
>['id', 'date_created', 'date_modified', 'author_str', 'per_curiam', 'joined_by_str', 'type', 'sha1', 'page_count', 'download_url', 'local_path', 'plain_text', 'html', 'html_lawbox', 'html_columbia', 'html_anon_2020', 'xml_harvard', 'html_with_citations', 'extracted_by_ocr', 'author_id', 'cluster_id']
['6027933', '2022-01-13 12:32:22.397073+00', '2025-07-24 02:38:12.390908+00', 'Mercure', 'f', '', '020lead', '789f4cb75ed3c58af8bdaa69269e8e4834c119a2', '', '', '', '', '', '', '', '', '&lt;opinion type=\\majority\\">']
['&lt;author id=\\"AX6\\">—Mercure', ' J.&lt;/author>']
['&lt;p id=\\"Aeq\\">Appeal from an order of the Supreme Court (Ferradino', ' J.)', ' entered May 23', ' 1997 in Saratoga County', ' which', ' &lt;em>inter alia', ' &lt;/em>denied defendant’s motion to hold plaintiff in contempt of court for failure to comply with the parties’ separation agreement.&lt;/p>']
['&lt;p id=\\"b752-6\\">The parties are the parents of two sons', ' born in 1978 and 1979. Their February 20', ' 1986 separation agreement was incorporated but not merged into a March 1986 judgment of divorce. Following several written modifications of the custody and visitation provisions of the separation agreement', ' in July 1996 defendant moved for an order of contempt based upon plaintiff’s alleged failure to comply with the terms of the separation agreement', ' as amended', ' and also sought to modify the judgment of divorce so as to grant her sole custody of the children', ' fix child support and make an award of counsel fees on that application. Supreme Court issued a temporary order', ' entered September 3', ' 1996', ' granting defendant primary physical custody of the children and directing plaintiff to pay weekly child support of $84.45 per child in accordance with the February 20', ' 1986 separation agreement and $1', '000 in counsel fees to defendant’s attorney. Thereafter', ' plaintiff moved and defendant cross-moved to modify the support terms of the temporary &lt;page-number citation-index=\\"1\\" label=\\"727\\">*727&lt;/page-number>order. Following conferences and settlement negotiations', ' plaintiffs counsel advised Supreme Court that the parties had come to terms on child support and requested that Supreme Court enter an order establishing plaintiffs support obligation in accordance with the parties’ agreement and dismissing the contempt motion. Despite defendant’s protestations that nothing more than a conditional agreement had been reached and that no stipulation of settlement had been placed on the record', ' Supreme Court entered an order fixing custody and support in accordance with the parties’ purported stipulation and denying defendant’s contempt motion. Defendant appeals.&lt;/p>']
>
>. . . 

ChatGPT helped me develop a strategy for tying the data I wanted altogether. Its idea was (in its own words):

1. **Start with opinion clusters, not opinions.** Because Court Listener groups majority opinions, concurrences, and dissents into “clusters” representing a single case disposition, the *cluster* is the right unit of analysis for case law. If you identify which clusters belonged to Washington courts, you can ignore the overwhelming majority of non-Washington data.

2. **Identify Washington clusters using metadata tables first.** Rather than touching the 50-GB opinions file immediately, first work with smaller, more structured tables (courts, dockets, and opinion clusters) to determine which `cluster_id`s corresponded to Washington state courts.

3. **Build a whitelist of Washington `cluster_id`s.** Once you have a definitive list of Washington-specific cluster IDs, treat that list as a filter and ignore everything else in the opinions table.

4. **Stream the opinions file once, filtering on the fly.** Instead of extracting or storing all opinion text, stream through `opinions-2025-12-02.csv.bz2` exactly once, row by row, and write out only those rows whose `cluster_id` appeared in my Washington whitelist.

### Opinion clusters
(incomplete)
### Identifying Washington clusters using metadata tables
(incomplete)
### Building the whe whitelist
(incomplete)
### Streaming the opinions file

In an ideal world, streaming a CSV file in Python would be as simple as my simple snippet above. And yet, we do not live in an ideal world. The data inside these CSV files is messy, especially the 50 GB opinions file. It comprises 15+ years of scraped data from various sources containing all sorts of XML/HTML tags, quotation marks and other problematic characters, and so forth. 

After inspecting bits and pieces of the downloaded compressed files through the command line, ChatGPT told me that Court Listener's files have their own "dialect," by which it means the symbols that signal the end and beginning of an item in the data, and the character used to escape characters that would, if unescaped, mean something, are atypical. After showing it [this post](https://github.com/freelawproject/courtlistener/discussions/6528#discussioncomment-14954403), ChatGPT told me that the dialect used is:

```python
quotechar='"'
quoting=csv.QUOTE_ALL
escapechar='\\'
doublequote=False
```



(to be continued...)
