This program matches names found by NER to a database of names and organizations.

# Usage

Three inputs are necessary:
* manifest file : file containing the full paths of each spark_NER output file.
* known entity data file : csv file containing person and organization data.
* output path : path to output directory. Each file specified in manifest creates a separate subdirectory.

Known entity data is loaded into memory, so executor memory size has to accomodate it.

# Execution
* First parameter: manifest file containing on each row the path of an input file/directory
* Second parameter: path to CSV file containing known entity data (description below)
* Third parameter: output path, target directory must not exist
Example:
> spark-submit --conf "spark.executor.memory=4g" /home/kaarelt/NER_data_matching/nerMatcher.py /home/kaarelt/manifest.txt hdfs:///csv/spark-input-29122015.csv hdfs:///user/kaarelt/output/outX 

# Input CSV Format
Each line of the known entity data CSV file should be in the following format. Use \N to mark a field as empty. 
>"NATION-ID","PERSON-ID","FIRSTNAME","LASTNAME","BIRTHDATE","ORGANIZATION-REGISTRY-ID","ORGANIZATION-NAME","RELATION-STARTDATE","RELATION-ENDDATE","RELATION-NAME"

Examples:
> "EE","123456789","FirstName","LastName","1901-01-29","123456","OrganizationName","2016-01-04","0000-00-00","ceo"
> "EE","123456789","FirstName","LastName","1901-01-29",\N,\N,\N,\N,\N
> "EE",\N,\N,\N,\N,"123456","OrganizationName","2016-01-04","0000-00-00","ceo"

# Result Contents
* pageId : String composed of protocol, host, path, parameters, date. Separated by "::"
* distinctNames : List of all distinct names detected from the page by ESTNLTK.
* singleMatchedEntities_(PER/ORG) : Names (and entity data) with exactly one matching known entity.
* crossmatchedEntities_(PER/ORG) : Names (and entity data) with more than one matching known entity, but narrowed down because of occurring with another matched entity of the other type.
* nonCrossmatchedNames_(PER/ORG) : Names where narrowing down was not possible, therefore more than one matching entity exists.
* matchlessNames : Names where no matches exist.
* excludedNames : Names which were excluded from matching based on exclusion lists.
* notMatchableNames_(PER/ORG) : Normally empty. Names which were not included to other lists (because of errors).

