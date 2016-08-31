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
See "examples" directory.