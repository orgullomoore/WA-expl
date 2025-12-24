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

ChatGPT helped me develop a strategy for tying the data I wanted together. Its idea was (in its own words):

1. **Start with opinion clusters, not opinions.** Because Court Listener groups majority opinions, concurrences, and dissents into “clusters” representing a single case disposition, the *cluster* is the right unit of analysis for case law. If you identify which clusters belong to Washington courts, you can ignore the overwhelming majority of non-Washington data.

2. **Identify Washington clusters using metadata tables first.** Rather than touching the 50-GB opinions file immediately, first work with smaller, more structured tables (courts, dockets, and opinion clusters) to determine which `cluster_id`s corresponded to Washington state courts.

3. **Build a whitelist of Washington `cluster_id`s.** Once you have a definitive list of Washington-specific cluster IDs, treat that list as a filter and ignore everything else in the opinions table.


4. **Stream the opinions file once, filtering on the fly.** Instead of extracting or storing all opinion text, stream through `opinions-2025-12-02.csv.bz2` exactly once, row by row, and write out only those rows whose `cluster_id` appeared in the Washington whitelist.

### Opinion clusters
#### Court abbreviations
First, I needed to determine the abbreviations Court Listener uses for the courts within my scope. As the documentation says: "Because nearly every data type happens in a court, you'll probably need this table to import any other data type below." 

The header for this table contains the following columns:
```bash
=== courts CSV HEADER ===
  0: id
  1: pacer_court_id
  2: pacer_has_rss_feed
  3: pacer_rss_entry_types
  4: date_last_pacer_contact
  5: fjc_court_id
  6: date_modified
  7: in_use
  8: has_opinion_scraper
  9: has_oral_argument_scraper
 10: position
 11: citation_string
 12: short_name
 13: full_name
 14: url
 15: start_date
 16: end_date
 17: jurisdiction
 18: notes
 19: parent_court_id
```
I used the following code to inspect the CSV and find the most likely to be Washington-specific:
```python
import bz2, csv, re
from collections import defaultdict

path = r"C:\Users\orgul\Downloads\courts-2025-12-02.csv.bz2"
# Heuristics to find WA-ish rows without assuming column names.
WA_PAT = re.compile(r"\b(Washington|Wash\.|WA)\b", re.IGNORECASE)
WA_ID_HINT = re.compile(r"\b(wash|washctapp|washterr|washag)\b", re.IGNORECASE)

with bz2.open(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
    reader = csv.reader(f)
    header = next(reader)

    print("=== courts CSV HEADER ===")
    for i, col in enumerate(header):
        print(f"{i:3d}: {col}")

    # Scan a chunk and collect rows that look WA-related
    matches = []
    for row in reader:
        joined = " | ".join(row)
        if WA_PAT.search(joined) or WA_ID_HINT.search(joined):
            matches.append(row)
            if len(matches) >= 50:
                break

print("\n=== FIRST 50 WA-LIKE ROWS (raw) ===")
for r in matches:
    print(r)
```

All that did for me was give me the dictionary I would need for my next step:

```python
WA = {"wash", "washctapp", "washag", "washterr"}
```
And honestly, I could have just gone to the Court Listener case law search page and selected Washington State courts in the "Select Jurisdictions" box and searched "anything", which would have taken me to this URL: https://www.courtlistener.com/?type=o&q=anything&type=o&order_by=score%20desc&court=wash%20washctapp%20washag%20washterr (note: "wash", "washctapp", "washag", and "washterr" are all there).

#### Docket IDs
Next, I needed a list of docket IDs belonging to Washington courts. As the documentation says: 
>"Dockets contain high-level case information like the docket number, case name, etc. This table contains many millions of rows and should be imported before the opinions data below. A docket can have multiple opinion clusters within it, just like a real life case can have multiple opinions and orders."

The following code was used:
```python
import bz2, csv, time

src = r"C:\Users\orgul\Downloads\dockets-2025-12-02.csv.bz2"
out = r"C:\Users\orgul\Downloads\wa_docket_ids.csv"

WA = {"wash", "washctapp", "washag", "washterr"}

t0 = time.time()
n_in = 0
n_out = 0

with bz2.open(src, "rt", encoding="utf-8", errors="replace", newline="") as f_in, \
     open(out, "w", encoding="utf-8", newline="") as f_out:

    r = csv.DictReader(f_in)
    w = csv.writer(f_out)
    w.writerow(["docket_id", "court_id"])

    for row in r:
        n_in += 1
        c = row.get("court_id", "")
        if c in WA:
            w.writerow([row["id"], c])
            n_out += 1

        if n_in % 5_000_000 == 0:
            dt = time.time() - t0
            print(f"...scanned {n_in:,} rows in {dt/60:.1f} min; matched {n_out:,}")

dt = time.time() - t0
print(f"\nDONE. Scanned {n_in:,} rows; matched {n_out:,}")
print(f"Output: {out}")
print("OK: WA docket extract complete")
```

That produced this file, which is a simple two-column CSV with 134,764 rows–the docket ID in the first column, and the court code (e.g., wash = Supreme Court of Washington; washctapp = Washington Court of Appeals) in the second column: [wa_docket_ids.csv](https://raw.githubusercontent.com/orgullomoore/WA-expl/refs/heads/main/wa_docket_ids.csv) 

### Identifying Washington clusters using opinion-clusters table
The next step was to find out what clusters were associated with my docket IDs. As the documentation says: "Clusters serve the purpose of grouping dissenting and concurring opinions together. Each cluster tends to have a lot of metadata about the opinion(s) that it groups together." In other words, if you want an opinion, you need a cluster ID. And as we know, if you want a cluster ID, you need a docket ID (and if you want a docket ID, you need a court abbreviation). 

My opinion-clusters-2025-12-02.csv.bz2 file had the following header:
```bash
=== OPINION CLUSTERS CSV HEADER ===
  0: id
  1: date_created
  2: date_modified
  3: judges
  4: date_filed
  5: date_filed_is_approximate
  6: slug
  7: case_name_short
  8: case_name
  9: case_name_full
 10: scdb_id
 11: scdb_decision_direction
 12: scdb_votes_majority
 13: scdb_votes_minority
 14: source
 15: procedural_history
 16: attorneys
 17: nature_of_suit
 18: posture
 19: syllabus
 20: headnotes
 21: summary
 22: disposition
 23: history
 24: other_dates
 25: cross_reference
 26: correction
 27: citation_count
 28: precedential_status
 29: date_blocked
 30: blocked
 31: filepath_json_harvard
 32: filepath_pdf_harvard
 33: docket_id
 34: arguments
 35: headmatter
```

To see what I was working with, I created the following code to output the first 25 Washington-specific cluster rows as JSON objects:

```python
import bz2, csv, time, sys
t0 = time.time()
import json

# IMPORTANT: allow very large CSV fields (CourtListener text columns)
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

DOCKET_IDS = r"C:\Users\orgul\Downloads\wa_docket_ids.csv"
INFILE     = r"C:\Users\orgul\Downloads\opinion-clusters-2025-12-02.csv.bz2"
def output_25_WA_clusters():
    # Define the headers
    HEADERS = [
        'id', 'date_created', 'date_modified', 'judges', 'date_filed', 
        'date_filed_is_approximate', 'slug', 'case_name_short', 'case_name', 
        'case_name_full', 'scdb_id', 'scdb_decision_direction', 'scdb_votes_majority', 
        'scdb_votes_minority', 'source', 'procedural_history', 'attorneys', 
        'nature_of_suit', 'posture', 'syllabus', 'headnotes', 'summary', 
        'disposition', 'history', 'other_dates', 'cross_reference', 'correction', 
        'citation_count', 'precedential_status', 'date_blocked', 'blocked', 
        'filepath_json_harvard', 'filepath_pdf_harvard', 'docket_id', 'arguments', 
        'headmatter'
    ]
    
    # Load WA docket ids
    wa = set()
    with open(DOCKET_IDS, "r", encoding="utf-8", errors="replace", newline="") as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if row:
                wa.add(row[0])
    print(f"Loaded WA docket_ids: {len(wa):,} in {time.time()-t0:.2f}s")
    
    scanned = 0
    matched = 0
    last = time.time()
    last_scanned = 0
    
    with bz2.open(INFILE, "rt", encoding="utf-8", errors="replace", newline="") as fin:
        reader = csv.reader(fin, quotechar='"', escapechar='\\', doublequote=False)
        DOCKET_COL = 33  # docket_id index
        
        for row in reader:
            scanned += 1
            if scanned % 500 == 0:
                now = time.time()
                dt = now - last
                rate = (scanned - last_scanned) / dt if dt > 0 else 0
                print(f"...scanned {scanned:,} rows; matched {matched:,} | chunk rate {rate:,.0f} rows/sec | elapsed {(now-t0)/60:.1f} min")
                last = now
                last_scanned = scanned
            
            if len(row) > DOCKET_COL and row[DOCKET_COL] in wa:
                matched += 1
                print(f"Matched #{matched} out of {scanned} rows scanned.")
                
                # Create JSON object with field names
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(HEADERS):
                        row_dict[HEADERS[i]] = value
                
                print(json.dumps(row_dict, indent=2))
                
                if matched >= 25:
                    break
output_25_WA_clusters()
print(f"Output 25 Washington clusters in {time.time()-t0:.2f}s")
```
A typical cluster row looks like this:

```json
{
  "id": "3160512",
  "date_created": "2015-12-07 22:09:07.521221+00",
  "date_modified": "2024-11-16 07:29:27.979384+00",
  "judges": "Schindler, Dwyer, Lau",
  "date_filed": "2015-12-07",
  "date_filed_is_approximate": "f",
  "slug": "state-of-washington-v-jorge-luis-lizarraga",
  "case_name_short": "",
  "case_name": "State Of Washington v. Jorge Luis Lizarraga",
  "case_name_full": "The State of Washington, Respondent, v. Jorge Luis Lizarraga, Appellant",
  "scdb_id": "",
  "scdb_decision_direction": "",
  "scdb_votes_majority": "",
  "scdb_votes_minority": "",
  "source": "CU",
  "procedural_history": "",
  "attorneys": "Lila J. Silverstein (of Washington Appellate Project), for appellant., Daniel T. Satterberg, Prosecuting Attorney, and Dennis J. McCurdy, Deputy, for respondent.",
  "nature_of_suit": "",
  "posture": "",
  "syllabus": "",
  "headnotes": "",
  "summary": "",
  "disposition": "",
  "history": "",
  "other_dates": "",
  "cross_reference": "",
  "correction": "",
  "citation_count": "70",
  "precedential_status": "Published",
  "date_blocked": "",
  "blocked": "f",
  "filepath_json_harvard": "law.free.cap.wash-app.191/530.12460962.json",
  "filepath_pdf_harvard": "harvard_pdf/3160512.pdf",
  "docket_id": "3018707",
  "arguments": "",
  "headmatter": "<docketnumber id=\"b560-4\">\n    [No. 71532-1-I.\n   </docketnumber><court id=\"AFye\">\n    Division One.\n   </court><decisiondate id=\"ADkP\">\n    December 7, 2015.]\n   </decisiondate><br><parties id=\"b560-5\">\n    The State of Washington,\n    <em>\n     Respondent,\n    </em>\n    v. Jorge Luis Lizarraga,\n    <em>\n     Appellant.\n    </em>\n</parties><br><attorneys id=\"b563-10\">\n<span citation-index=\"1\" class=\"star-pagination\" label=\"533\"> \n     *533\n     </span>\n<em>\n     Lila J. Silverstein\n    </em>\n    (of\n    <em>\n     Washington Appellate Project),\n    </em>\n    for appellant.\n   </attorneys><br><attorneys id=\"b563-11\">\n<em>\n     Daniel T. Satterberg, Prosecuting\n    </em>\n    Attorney, and\n    <em>\n     Dennis J. McCurdy, Deputy,\n    </em>\n    for respondent.\n   </attorneys>"
}
```

The cluster ID is the key to getting to an opinion from Court Listener. A cluster ID will take you directly to an opinion: https://www.courtlistener.com/opinion/3160512/any-slug-will-do/ takes you directly to the opinion referenced above (*State v. Lizarraga*, 364 P.3d 810, 191 Wash. App. 530 (2015)).

### Building the whe whitelist
So, for my initial task of identifying which opinions were within the clusters that were within the dockets that were associated with the courts within the scope of my project, I just needed column 1 (the cluster ID) from every row where column 33 (the docket ID) was in my previously-generated list of docket IDs associated with Washington courts. 

I accomplished that with the following code:
```python
import bz2, csv, time, os, sys

# Bump CSV field limit (some opinions fields are huge)
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    # Windows/Python sometimes needs a smaller int
    csv.field_size_limit(2_000_000_000)

OPINIONS_BZ2 = r"C:\Users\orgul\Downloads\opinions-2025-12-02.csv.bz2"
WA_CLUSTERS  = r"C:\Users\orgul\Downloads\wa_opinion_clusters.csv"
OUT_CSV      = r"C:\Users\orgul\Downloads\wa_opinions.csv"

LOG_EVERY = 1_000_000

t0 = time.time()

# Load WA cluster_ids
wa = set()
with open(WA_CLUSTERS, "r", encoding="utf-8", newline="") as f:
    r = csv.reader(f)
    next(r, None)
    for row in r:
        if row:
            wa.add(row[0])
print(f"Loaded WA cluster_ids: {len(wa):,} in {time.time()-t0:.2f}s")

matched = 0
scanned = 0
last = time.time()

with bz2.open(OPINIONS_BZ2, "rt", encoding="utf-8", errors="replace", newline="") as f:
    r = csv.reader(f, quoting=csv.QUOTE_ALL, escapechar="\\")
    header = next(r)
    idx_cluster = header.index("cluster_id") if "cluster_id" in header else (len(header) - 1)

    with open(OUT_CSV, "w", encoding="utf-8", newline="") as out:
        w = csv.writer(out, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)

        for row in r:
            scanned += 1
            if len(row) != len(header):
                continue

            cid = row[idx_cluster]
            if cid in wa:
                w.writerow(row)
                matched += 1

            if scanned % LOG_EVERY == 0:
                now = time.time()
                rate = LOG_EVERY / (now - last)
                elapsed_min = (now - t0) / 60
                print(f"...scanned {scanned:,} rows; matched {matched:,} | rate {rate:,.0f} rows/sec | elapsed {elapsed_min:.1f} min")
                last = now

dt = time.time() - t0
print(f"\nDONE. Scanned {scanned:,} rows; matched {matched:,}")
print(f"Output: {OUT_CSV}")
print(f"Elapsed: {dt/60:.2f} min")
print("OK: WA opinions extract complete")
```
That left me with a very large file (68.6 MB) containing all the cluster rows (134,632 in number) in my scope: [wa_opinion_clusters.csv](https://huggingface.co/datasets/orgullomoore/wa_opinion_clusters/resolve/main/wa_opinion_clusters.csv)

### Streaming the opinions file
Lastly, I had to find all the opinions (or "sub_opinions", as Court Listener calls them) associated with my 134,632 clusters. By way of example, *Roe v. Wade*, [410 U.S. 113](https://www.courtlistener.com/c/us/410/113) (1973) contains three "sub_opinions" in Court Listener's database: the majority opinion authored by Justice Blackmun (opinion ID: 9425157), the concurring opinion authored by Justice Stewart (opinion ID: 9425158), and the dissenting opinion authored by Justice Rehnquist (opinion ID: 9425159).

The header for the opinions file is as follows:
```json
{
  "0": "id",
  "1": "date_created",
  "2": "date_modified",
  "3": "author_str",
  "4": "per_curiam",
  "5": "joined_by_str",
  "6": "type",
  "7": "sha1",
  "8": "page_count",
  "9": "download_url",
  "10": "local_path",
  "11": "plain_text",
  "12": "html",
  "13": "html_lawbox",
  "14": "html_columbia",
  "15": "html_anon_2020",
  "16": "xml_harvard",
  "17": "html_with_citations",
  "18": "extracted_by_ocr",
  "19": "author_id",
  "20": "cluster_id"
}
```

This was, by far, the most difficult part. The task was, in principle, very simple: I need all rows where column 20 matched one of my 134,632 cluster IDs that I had collected from my previous step. The problem that arose is how to find where one row ends and another begins, when the opinions contain all sorts of XML/HTML tags, quotation marks and other problematic characters, and so forth.

After inspecting bits and pieces of the downloaded compressed files through the command line, ChatGPT told me that Court Listener's files have their own "dialect," by which it means the symbols that signal the end and beginning of an item in the data, and the character used to escape characters that would, if unescaped, mean something, are atypical. After showing it [this post](https://github.com/freelawproject/courtlistener/discussions/6528#discussioncomment-14954403), ChatGPT told me that the dialect used is:

```python
quotechar='"'
quoting=csv.QUOTE_ALL
escapechar='\\'
doublequote=False
```

Even after knowing of this dialect, neither ChatGPT nor Claude were able to come up with a way to give me a file that I could import into my database. After many, many failed attempts–each taking hours long–ChatGPT told me quite curtly:
>Here is the clean diagnosis and the only sane way forward.
>
>What is actually wrong (now proven)
>
>Your file is **not a CSV of opinions**.
>
>It is a **raw text dump where entire opinion bodies are written as standalone physical lines**, *outside of any CSV row structure*.
>
>Evidence (from your output):
>
>=== lines 77–82 ===
>
>77    requiring him to appear restrained at sentencing without conducting an individualized
>
>78
>
>79    assessment, (3) erred in failing to apply a change to RCW 9.94A.525 when calculating Miller’s
>
>80
>
>81    offender score, (4) failed to strike the second degree felony murder conviction from Miller’s
>
>82
>
>These are **plain prose lines**, not:
>
>id,date_created,date_modified,...
>
>So MySQL is not “mis-parsing” anything.
>
>**The file is structurally invalid as CSV.**
>
> What actually exists in the source file is:
>
>[id, metadata fields..., html/plain_text field starts]
>
>newline>
>
>newline>
>
>newline>
>
>more text>
>
>more text>
>
>...
>
>When your cleaner split on newlines, **it created thousands of fake “rows”** where the *first column became `<p id="...">It would seem`*, which then hit MySQL as:
>
>id = "<p id=...>"
>
>Hence:
>
>Incorrect integer value for column 'id'
>
>This is not a MySQL issue, a quoting issue, or a datatype issue.
>
>It is a **record boundary problem**.
>
>---
>
>##### ✅ The only correct interpretation
>
>Your file is effectively:
>
>* CSV header (real)
>* Some valid CSV rows
>* Then **unescaped multiline text blobs that were never enclosed in quotes**
>* Then more CSV-looking fragments
>* You must **reconstruct logical records**, not parse lines.
>
> . . .
>
>🚫 What will NEVER work (stop trying)
>
>* `LOAD DATA INFILE`
>* Tweaking delimiters
>* Escaping quotes
>* Switching CSV ↔ TSV
>* MySQL `FIELDS TERMINATED BY`
>* `IGNORE n LINES`
>
>Those tools assume valid records. You do not have valid records.
>---
>📌 Hard conclusion
>
>You are **not debugging MySQL**.
>
>You are **repairing a corrupted logical dataset**.
>
>Until the file has:
>* exactly **1 logical row per opinion**
>* with **multiline fields properly enclosed or reconstructed**
> 
> **No database load will succeed.**

Thus, I had to bring out the big guns and ask Gemini [3 Pro]. I had her review what ChatGPT had done, and she gave me the following solution:

```python
import csv
import bz2
import io
import sys
import time
import os
import datetime

# --- CONFIGURATION ---
CLUSTERS_PATH = r"C:\Users\orgul\WACL\wa_opinion_clusters.csv"
OPINIONS_PATH = r"C:\Users\orgul\WACL\opinions-2025-12-02.csv.bz2"
OUTPUT_PATH   = r"C:\Users\orgul\WACL\wa_opinions.csv"

# --- 1. MAXIMIZE FIELD LIMIT ---
# Essential for large legal texts
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int // 10)

print(f"--- WA OPINION EXTRACTOR ---")
print(f"Field limit: {max_int}")
print(f"Input:       {OPINIONS_PATH}")
print(f"Output:      {OUTPUT_PATH}")

# --- 2. LOAD WA CLUSTER IDS ---
print(f"\nLoading filter list from {CLUSTERS_PATH}...")
wa_cluster_ids = set()
try:
    with open(CLUSTERS_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "id" in row and row["id"]:
                wa_cluster_ids.add(row["id"])
    print(f"Loaded {len(wa_cluster_ids):,} unique WA cluster IDs.")
except Exception as e:
    print(f"CRITICAL ERROR loading clusters: {e}")
    sys.exit(1)

# --- 3. STREAMING EXTRACTION ---
file_size = os.path.getsize(OPINIONS_PATH)
print(f"\nStarting extraction on {file_size / (1024**3):.2f} GB file...")

start_time = time.time()
rows_processed = 0
rows_matched = 0

try:
    # Open raw for byte position tracking
    raw_file = open(OPINIONS_PATH, "rb")
    bz2_file = bz2.BZ2File(raw_file, "rb")
    text_wrapper = io.TextIOWrapper(bz2_file, encoding="utf-8", errors="replace", newline="")
    
    # CORRECT DIALECT: Postgres export format
    reader = csv.reader(
        text_wrapper,
        quotechar='"',
        escapechar='\\',
        doublequote=False,
    )

    out_file = open(OUTPUT_PATH, "w", encoding="utf-8", newline="")
    writer = csv.writer(
        out_file,
        quotechar='"',
        escapechar='\\',
        doublequote=False,
        quoting=csv.QUOTE_MINIMAL 
    )

    # Header Processing
    header = next(reader)
    writer.writerow(header)
    
    try:
        cluster_idx = header.index("cluster_id")
    except ValueError:
        cluster_idx = 20 # Fallback

    # Main Loop
    for row in reader:
        rows_processed += 1
        
        # Filter Logic
        if len(row) > cluster_idx:
            if row[cluster_idx] in wa_cluster_ids:
                writer.writerow(row)
                rows_matched += 1

        # Status Update (Time-based to prevent log spam)
        if rows_processed % 2000 == 0:
            current_pos = raw_file.tell()
            progress = current_pos / file_size
            elapsed = time.time() - start_time
            
            if progress > 0.00001:
                eta_seconds = (elapsed / progress) - elapsed
                eta_str = str(datetime.timedelta(seconds=int(eta_seconds)))
            else:
                eta_str = "Calc..."
                
            rate = rows_processed / elapsed if elapsed > 0 else 0
            
            # Print status line
            sys.stdout.write(f"\rProgress: {progress*100:.2f}% | Matched: {rows_matched:,} | Rate: {rate:.0f}/s | ETA: {eta_str}   ")
            sys.stdout.flush()

except KeyboardInterrupt:
    print("\n\nUser cancelled.")
except Exception as e:
    print(f"\n\nFATAL ERROR at row {rows_processed:,}: {e}")
finally:
    if 'out_file' in locals(): out_file.close()
    if 'bz2_file' in locals(): bz2_file.close()
    if 'raw_file' in locals(): raw_file.close()
    
    total_time = time.time() - start_time
    print(f"\n\n--- COMPLETE ---")
    print(f"Rows Scanned: {rows_processed:,}")
    print(f"Rows Matched: {rows_matched:,}")
    print(f"Time Elapsed: {str(datetime.timedelta(seconds=int(total_time)))}")
```

I asked Gemini to articulate what the issue was with the prior attempts specifically, and she said:
<details>
	<summary>Gemini's explanation</summary>

### The Problem
The CourtListener opinions file (`opinions-2025-12-02.csv.bz2`) is a raw PostgreSQL database export, not a standard Excel-style CSV.
1.  **Dialect Mismatch:** Standard CSV parsers expect double-quotes (`""`) to escape quotes inside text. This file uses backslashes (`\"`) to escape quotes. When read with standard settings, Python's CSV reader got confused by quotes inside the opinion text, treating newlines inside the text as new rows, which destroyed the file structure.
2.  **Field Size Limits:** Legal opinions are massive. Some single fields (the opinion text) exceed Python's default CSV limit of 131,072 characters, causing the script to crash with `_csv.Error: field larger than field limit`.

### The Solution
I created a robust streaming script that:
1.  **Configures the CSV Dialect Correctly:** Explicitly sets `escapechar='\\'` and `doublequote=False` to match the PostgreSQL format.
2.  **Increases Field Limits:** Dynamically sets `csv.field_size_limit` to the maximum integer your system can handle (`sys.maxsize`), preventing crashes on long opinions.
3.  **Streams Efficiently:** Reads the compressed BZ2 file incrementally (never loading more than a buffer into RAM) while tracking the raw byte position to calculate a real-time ETA based on file size processing rather than row counts.

### Proof It Works
We proved this via the **Spot Check** script (`Gemini2.py` in your history), which successfully:
1.  Scanned the file until it found the first Washington opinion (Row 8).
2.  Extracted the text without error.
3.  **Verified the column count:** It confirmed the row had exactly **21 columns**, matching the header. If the parsing were still broken, this number would have been random (e.g., 5 or 50) due to split lines.

You are now running the final script with a progress bar and ETA.
</details>
Et voilà ! Around 4.5 hours later, I had a 4.22 GB file, [wa_opinions_v3_2025-12-24.csv](https://huggingface.co/datasets/orgullomoore/WA_opinions/resolve/main/wa_opinions_v3_2025-12-24.csv), which contained all my opinions and were ready to import into mysql. I then used the following code to import my data into my table:

```python
import csv
import sys

# --- CONFIG ---
CSV_FILE = "wa_opinions_v3_2025-12-24.csv"
BATCH_SIZE = 20  # Conservative batch size for stability

def mysql_escape(value):
    if value is None:
        return "NULL"
    value = value.replace("\\", "\\\\") \
                 .replace("'", "\\'") \
                 .replace('"', '\\"') \
                 .replace("\n", "\\n") \
                 .replace("\r", "\\r")
    return f"'{value}'"

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: python csv_to_sql.py <table_name>\n")
        sys.exit(1)
    
    table_name = sys.argv[1]
    
    # Increase CSV Field Limit (Fix for 131072 error)
    max_int = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max_int)
            break
        except OverflowError:
            max_int = int(max_int // 10)
    
    columns = [
        "id", "date_created", "date_modified", "author_str", 
        "per_curiam", "joined_by_ids", "type", "sha1", 
        "page_count", "download_url", "local_path", "plain_text", 
        "html", "html_lawbox", "html_columbia", "html_with_citations", 
        "extracted_by_ocr", "author_id", "cluster_id"
    ]
    col_str = ", ".join(columns)
    
    sys.stderr.write(f"Reading {CSV_FILE}...\n")
    
    try:
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f, quotechar='"', escapechar='\\', doublequote=False)
            
            try:
                header = next(reader)
            except StopIteration:
                return

            batch_values = []
            
            for row in reader:
                if len(row) < 21: continue
                
                # MAPPING
                d_created = row[1].split("+")[0]
                d_modified = row[2].split("+")[0]
                per_curiam = 1 if row[4] == 't' else 0
                ocr = 1 if row[18] == 't' else 0
                pg_count = row[8] if row[8] else None
                auth_id = row[19] if row[19] else None
                
                vals = [
                    row[0], d_created, d_modified, row[3], 
                    per_curiam, row[5], row[6], row[7], 
                    pg_count, row[9], row[10], row[11], 
                    row[12], row[13], row[14], row[17], 
                    ocr, auth_id, row[20]
                ]
                
                escaped_vals = []
                for v in vals:
                    if isinstance(v, int):
                        escaped_vals.append(str(v))
                    else:
                        escaped_vals.append(mysql_escape(v))
                
                batch_values.append(f"({', '.join(escaped_vals)})")
                
                if len(batch_values) >= BATCH_SIZE:
                    print(f"INSERT INTO {table_name} ({col_str}) VALUES {', '.join(batch_values)};")
                    batch_values = []

            if batch_values:
                print(f"INSERT INTO {table_name} ({col_str}) VALUES {', '.join(batch_values)};")

    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
```
Et voilà !  After a few minutes, I had 147,782 rows inserted into my database. 

## Lesson learned
CourtListener bulk “opinions” is a PostgreSQL <code>COPY ... CSV</code> export. You must parse it with 
```python
quotechar='"'
<code>doublequote=False
escapechar='\\'
```
and raise <code>csv.field_size_limit</code> (opinions routinely exceed 131k chars). If you parse with default CSV settings, the parser will lose quoting context when it encounters backslash-escaped quotes, and then newlines inside the opinion text will be misread as row boundaries—producing “rows” that start with &lt;p id=...> and breaking downstream imports.

