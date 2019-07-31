![](images/architecture-overview.png)

# The principal components of this body of software:

1. Ingest
2. RESTful Services

## Ingest
The ingestion component of the code is responsible for the following:
 1. Reads zipped tar files that are parsed for apiKey-value pairs,
 2. Generates a Row identifier based on
    * Corpus version,
    * Project name,
    * Creation date,
    * Repository,
    * UUID,
 3. Inserts each RowID/<Key/Value> pair in a projectMetadata column family,
 4. Inserts a rotating RowId that includes topics and languages in addition to the RowId in (3) forming a partition index. 

## RESTful Services
The RESTful services supply the following endpoints:
* Account creation,
* Category Browsing,
* Searching for projects using boolean expressions,
* Basic statistics on metadata valuse (min, max, counts, and average),
* Reading and writing of exclusive and nonexclusive metadata 


## Source Organization
#### model
<details><summary>accumulo</summary>
<p>
Wraps calls using accumulo api for scans (reads) and mutations (writes).
</p>
</details>

<details><summary>entities</summary>
<p>
Entities folder predominantly contains a hierarchy of return types for JSON producing endpoints:

* Corpus Metadata
* Project Metadata
* User Metadata
</p>
</details>

#### muse_utils
<details><summary>AccountServices</summary>
<p>
Code responsible for creating accounts.
</p>
</details>
<details><summary>App</summary>
<p>
Main entry point of the app.
</p>
</details>
<details><summary>Ingest</summary>
<p>
Ingest is responsible for populating Accumulo tables.
</p>
</details>
<details><summary>Security</summary>
<p>
Handles authentication and authorization.
</p>
</details>

#### resources
<details><summary>Accounts</summary>
<p>
Code responsible for creating accounts.
</p>
</details>
<details><summary>CategoryBrowsing</summary>
<p>
Mostly UI support, Help, query, query validation,.
</p>
</details>
<details><summary>CategoryItem</summary>
<p>
Serves category-based projects.
</p>
</details>
<details><summary>Content</summary>
<p>
(Untested) Returns content files
</p>
</details>
<details><summary>Metadata</summary>
<p>
Serves metadata values, 
</p>
</details>
<details><summary>ResourceBase</summary>
<p>
Base class for resource classes.
</p>
</details>
<details><summary>Stats</summary>
<p>
Returns basic statistics on metadata keys and values.</p>
</details>
<details><summary>Summary</summary>
<p>
Counts the total number of metadata keys.
</p>
</details>
<details><summary>UserData</summary>
<p>
Populates accumulo with data written by accounts.
</p>
</details>

