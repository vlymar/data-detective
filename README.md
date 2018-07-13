# Data Detective

I used this project to learn Scala and experiment with some of the functional programming techniques in [https://www.manning.com/books/functional-programming-in-scala](https://www.manning.com/books/functional-programming-in-scala). 

### Overview
Given a large collection of JSON documents, Data Detective efficiently generates a schema and aggregate statistics over the data. The goal is to be able to run data detective on n machines/sets of documents and then aggregate the intermediate results. 

Although I did use data detective while building an ETL system at Scribd for a [large set of court document metadata](https://www.courtlistener.com/api/bulk-info/), it is more of a personal learning project than a production ready tool. There are most certainly bugs present. The output is raw and not easily human readable (yet).

### Usage
Note: these are temporary instructions. From the repo directory:

`$ sbt "run datadir/ out.txt"`

### Algorithm

Data Detective (DD) accepts a directory as an argument. It then builds a stream of the files in the directory and lazily does the following:
1. parses the JSON in each file
2. converts the parsed JSON into an intermediate data structure (a type of `ValueAggregate`) that represents the schema and statistics of that JSON file.
3. merges the aggregations together.
4. Serializes the aggregation to JSON and prints it.

The intermediate representation is designed to take up constant space\*, and to be mergeable with other intermediate representations generated by separate processes. This allows the algorithm to be parallelizable.

\* Its a while since I've been in school, so this is not _totally_ accurate. The intermediate representation actually scales with the size of the overall schema of the dataset. This is negligible relative to the size of the dataset itself though, unless your schema is huge and your dataset is tiny :)

### Example

Data source: https://www.courtlistener.com/api/bulk-data/opinions/tax.tar.gz

I've downloaded Courlistener's bulk dataset for the United States tax court and extracted the json documents into a `tax-data/` dir. I then run data-detective on this dataset, instructing it to output the aggregation to `tax-agg.json`.

```
$ sbt "run tax-data tax-agg.json"
[info] Loading project definition from /Users/victor/src/data-detective/project
[info] Set current project to data-detective (in build file:/Users/victor/src/data-detective/)
[info] Running Main tax-data tax-agg.json
input:
tax-data; tax-agg.json

aggregating 1025 files
[success] Total time: 2 s, completed Jul 13, 2018 12:59:52 PM
```

`tax-agg.json` can be found here: https://gist.github.com/vlymar/495a4be0cfa261c0a679f9169e947a68 .

Note that this is a direct serialization of the intermediate aggregate data structure. It is designed to be merged with other aggregations, not for human readability. If you squint you can start to see some useful info though. Lets look at a snippet:

```json
"type" : "json",
"statistics" : {
  "count" : 1025
},
"attributes" : {
  "sha1" : {
    "type" : "string",
    "values" : [ ],
    "statistics" : {
      "minLen" : 40,
      "maxLen" : 40,
      "avgLen" : 40,
      "numBlank" : 0,
      "overEnumLimit" : true,
      "count" : 1025
    }
  },
  "author" : {
    "type" : "null",
    "statistics" : {
      "count" : 1025
    }
  },
```

Here's what I can tell you from that:
- there are 1025 json documents in this dataset
- every document has a "sha1" field
- the sha1 field has a min/max/avg length of 40 characters
- the sha1 field is never blank
- every document has an "author" field
- the author field ALWAYS has a null value

Here's another snippet:

```json
"html_lawbox" : {
  "type" : "multiple_types",
  "count" : 1025,
  "string" : {
    "type" : "string",
    "values" : [ "" ],
    "statistics" : {
      "minLen" : 0,
      "maxLen" : 0,
      "avgLen" : 0,
      "numBlank" : 886,
      "overEnumLimit" : false,
      "count" : 886
    }
  },
  "null" : {
    "type" : "null",
    "statistics" : {
      "count" : 139
    }
  }
},
```

The `html_lawbox` field is either:
- an empty string (886 times)
- `null` (139 times)

```json
"type" : {
  "type" : "string",
  "values" : [ "010combined" ],
  "statistics" : {
    "minLen" : 11,
    "maxLen" : 11,
    "avgLen" : 11,
    "numBlank" : 0,
    "overEnumLimit" : false,
    "count" : 1025
  }
},
```

The `type` field _always_ has a value of "010combined". If a field has few distinct values, data-detective will preserve that set and present it to you. This is particularly useful when a field represents an enum.


### Alternatives
The incredible [jq](https://stedolan.github.io/jq/). jq doesn't quite do the same thing as data-detective but you can use it to achieve the similar ends, probably more correctly :)
