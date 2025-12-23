# Data Extraction for WA Law Explorer
In this document, I will attempt to explain how I extracted data from Court Listener's [bulk legal data files](https://www.courtlistener.com/help/api/bulk-data/) for my [WA Law Explorer](http://170.205.38.113/cases/) project.

## The project
New to the State of Washington, I decided that I wanted to build a tool to intuitively explore its statutes and case law. My initial MVP was simply to (try to) download all Washington opinion texts available through Court Listener, and a copy of the Revised Code of Washington (RCW), and then parse the opinion text for citations to the RCW from the opinions to build out a database linking statutes to cases. 

## The RCW download
This was easy enough. With the assistance of AI vibe coding, I created rcw_spider.py, which crawled the Legislature's website to get the (~51.6k code sections)[https://huggingface.co/datasets/orgullomoore/RCW] and put those in a sqlite3 file. I later converted this to a parquet file and uploaded it to Hugging Face as a dataset.

## The Court Listener bulk data files
This was the hard part. I wanted only case law specific to Washington. Court Listener makes its "Bulk Legal Data" available for everyone to download, but the process is not intuitive. The files are exports from their database tables, which they run every month or so. There is no way to download only Washington opinions. You have to download everything from (https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/)[here] and then pick out what you're looking for. In addition, it's not all in one place. As their documentation says, they have separate files for (as relevant to my project):

- Courts - A "dump of court table and contains metadata about the courts we have in our system. Because nearly every data type happens in a court, you'll probably need this table to import any other data type below. We suggest importing it first." The filename looks something like "courts-2025-12-02.csv.bz2" and is 78.9 kB.
- Dockets - A collection of "high-level case information like the docket number, case name, etc." with "many millions of rows" that "should be imported before the opinions data[.]" The file looks something like "dockets-2025-12-02.csv.bz2" and is 4.5 GB.
- Opinion Clusters - A mapping of which majority opinions, concurrences, and dissents belong to single "clusters," i.e., the court's disposition of a particular case. The file looks something like "opinion-clusters-2025-12-02.csv.bz2" and is 2.3 GB.
- Opinions - The full text of the opinions. The file looks something like "opinions-2025-12-02.csv.bz2" and is 49.9 GB 
- Citations Map - A "narrow table that indicates which opinion cited which and how deeply."
- Parentheticals - A collection of explanatory parentheticals, which are useful for determining what proposition a given case is cited for e.g., the part in parenthesis in: "*Merrion v. Jicarilla Apache Tribe*, 455 U.S. 130, 137, 102 S.Ct. 894, 71 L.Ed.2d 21 (1982) (holding that only in Indian country may tribes exercise powers over nonmembers)."

If you just download the Opinions file, you will not have any information about what case you're looking at other than what you can discern from the text itself. 
(to be continued...)
