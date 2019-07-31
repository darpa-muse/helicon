# MUSE Browsing REST API

## Entities/Schema

These are items returned by public endpoints.


### ProjectMetadata


~~~~
{
    "mapValues"         :{},
    "arrayValues"       :[],
    "count"             :0,
    "key"               :"Generic Key",
    "value"             :"Generic Value",
    "url"               :"Called URL",
    "elapsedTimeMsec"   :0
}
~~~~


### CorpusMetadata

~~~~
{
    "mapValues":{},
    "arrayValues":[],
    "count":0,
    "key":"Generic Key",
    "value":"Generic Value",
    "url":"Called URL",
    "elapsedTimeMsec":0
}
~~~~

### UniqueCorpusMetadata


~~~~
{
    "values"            :[],
    "count"             :0,
    "key"               :"Holds a generic key",
    "value"             :"Holds a generic value.",
    "url"               :"Holds the called URL",
    "elapsedTimeMsec"   :0
}
~~~~

### MetadataValueStats

~~~~
{
    "variance"          :0.0,
    "min"               :0.0,
    "max"               :0.0,
    "std"               :0.0,
    "mean"              :0.0,
    "sum"               :0.0,
    "count"             :0.0,
    "mode"              :0.0,
    "key"               :"",
    "url"               :"",
    "elapsedTimeMsec"   :0
}
~~~~

### MuseResponse

~~~~
{
    "message"           :"This holds the message set by the endpoint",
    "key"               :"Since this is used in multiple contents, it holds the metadata key",
    "value"             :"Holds the value; typically a metadata value",
    "url"               :"This is the called URL",
    "elapsedTimeMsec"   :0
}
~~~~

### Examples

#####Custom search
~~~~
.../muse/search?q=<base64-encoded boolean search expression>
~~~~

#####List of available categories and their count (e.g., Languages, Topics, etc.

~~~~
.../muse/categories
~~~~


#####list of projects for a specific category (e.g., Java projects, etc.

~~~~
.../muse/categories/{category}
~~~~

#####Particular page list of project for a specific category (integer > 0)

~~~~
.../muse/categories/{category}/pages/{page}
~~~~

#####Metadata listing for a particular project

~~~~
.../muse/categories/{category}/pages/{page}/projects/{project}
~~~~

#####Metadata values for key (Use of pagesize/page: if looking for 1000 values, set pagesize=1000 and page=1)

~~~~
.../muse/metadata/{key}/pagesize/{pageSize}/page/{page}
~~~~

#####Unique metadata values for key (Use of pagesize/page: if looking for 1000 values, set pagesize=1000 and page=1)

~~~~
.../muse/metadata/{key}/pagesize/{pageSize}/page/{page}/unique
~~~~

#####Count of unique metadata items

~~~~
.../muse/stats/count/metadata/{key}/pagesize/{pageSize}/page/{page}
~~~~

#####Average value of metadata

~~~~
.../muse/stats/ave/metadata/{key}/pagesize/{pageSize}/page/{page}
~~~~

#####Minimum and maximum (as a vector) of the projoct metadata key value

~~~~
.../muse/stats/minmax/metadata/{key}/pagesize/{pageSize}/page/{page}
~~~~

#####Get the link to binary (code or metadata) content

~~~~
.../muse/categories/{category}/{categoryItem}/pages/{page}/projects/{projectRow}/content/{type: 'code' or 'metadata'}
~~~~


