This program matches names found by NER to a database of names and organizations.

# Usage

Three inputs are necessary:
* manifest file : file containing the full paths of each spark_estnltk result file (webpage data containing unmatched names from that page).
* known entity data file/directory : CSV file(s) containing person and organization data. See below for format.
* output path : path to output directory. Must be non-existing on execution.

Known entity data is loaded into memory, so executor memory size has to accomodate it.

# Execution
* -m, --manifest : manifest file containing on each row the path of an input file/directory
* -k, --known : path to CSV file containing known entity data (description below)
* -o, --out : output path, target directory must not exist
On first execution, the known entity database has to be created: 
> spark-submit --conf "spark.executor.memory=4g" --conf "spark.kryoserializer.buffer.max=1024m" named_entity_to_known_entity_matching/nerMatcher.py -m benchmarking/manifest_matching02.txt -k hdfs:/user/kaarelt/csv -x hdfs:/user/kaarelt/exclude -o hdfs:/user/kaarelt/benchmarking/firstfive/matching02x155 --processKnown

On following executions, the database can be reused:
> spark-submit --conf "spark.executor.memory=4g" --conf "spark.kryoserializer.buffer.max=1024m" named_entity_to_known_entity_matching/nerMatcher.py -m benchmarking/manifest_matching02.txt -k hdfs:/user/kaarelt/csv -x hdfs:/user/kaarelt/exclude -o hdfs:/user/kaarelt/benchmarking/firstfive/matching02x155


# Additional Parameters
* -m, --manifest : Manifest file path (see above)
* -k, --known : Known entities CSV path (see above and below)
* -o, --out : Output path (see above)
* -x, --exclude : Directory containing CSV file of names to exclude from unknown entity names (see below)
* --processKnown : Read and process known entity data from CSV file(s) in knownEntitiesPath, then save results to knownEntitiesPath/KEDict_PER and KEDict_ORG. If omitted, assume known entities are already processed and read them from knownEntitiesPath/KEDict_PER and KEDict_ORG.  
* --lemmatizeKnown : (EXPERIMENTAL) Performs ESTNLTK lemmatization on known entity names before matching. Forces --processKnown if not already included in parameters.
* --explodeOutputForNames : For all unknown entity names, output URI-name pairs to explodedNames directory in output directory
* --explodeOutputForMatches : For all successful matches, output URI-match pairs to explodedSuccesses_<type> directories 

# Manifest File
Simple text containing a full path of a known entities CSV file on each row.
Example:
>/user/kaarelt/benchmarking/firstfive/nltk_processed00
>/user/kaarelt/benchmarking/firstfive/nltk_processed02

# Known Entities CSV Format
Each line of a known entity data CSV file should be in the following format. Use \N to mark a field as empty. 
>"NATION-ID","PERSON-ID","FIRSTNAME","LASTNAME","BIRTHDATE","ORGANIZATION-REGISTRY-ID","ORGANIZATION-NAME","RELATION-STARTDATE","RELATION-ENDDATE","RELATION-NAME"

Examples:
> "EE","123456789","FirstName","LastName","1901-01-29","123456","OrganizationName","2016-01-04","0000-00-00","ceo"
> "EE","123456789","FirstName","LastName","1901-01-29",\N,\N,\N,\N,\N
> "EE",\N,\N,\N,\N,"123456","OrganizationName","2016-01-04","0000-00-00","ceo"

# Exclusion/synonym Format
Each line of the exclusion/synonym CSV file should be in the following format:
>"name","tag1","synonym","tag2"

If tag1 is any from ["event","famous_per","famous_org","loc","product","nan"], then the name is excluded from matching.
If tag1 is "synonym", instances of name are replaced by value from synonym.
Any of the fields can be empty.
Examples:
> Eesti,loc,,
> TÜ,synonym,Tartu Ülikool,

# Result Contents
* pageId : String composed of protocol, host, path, parameters, date. Separated by "::"
* distinctNames : List of all distinct names detected from the page by ESTNLTK.
* singleMatchedEntities_(PER/ORG) : Names (and entity data) with exactly one matching known entity.
* crossmatchedEntities_(PER/ORG) : Names (and entity data) with more than one matching known entity, but narrowed down because of occurring with another matched entity of the other type.
* nonCrossmatchedNames_(PER/ORG) : Names where narrowing down was not possible, therefore more than one matching entity exists.
* matchlessNames : Names where no matches exist.
* excludedNames : Names which were excluded from matching based on exclusion lists.
* notMatchableNames_(PER/ORG) : Normally empty. Names which were not included to other lists (because of errors).

