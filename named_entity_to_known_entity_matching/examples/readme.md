# Result format
* hostname: host name of page
* date : time of page collection
* path : path of page on the host
* allNamedEntities : List of all names detected on the page. These may or may not have matching entities in the database.
* singleMatchedEntities_(PER/ORG) : Names that have a single matching named entity in the database, along with the data of the named entity
* crossMatchedEntities_(PER/ORG) : Names that have multiple matches in the database, but combined with a successful match from the other type, can be narrowed down to a single named entity.
* nonCrossmatchedNames_(PER/ORG) : Names that cannot be narrowed down by cross-referencing the matched names of the other type.
* unmatchedNamedEntityKeys : list of normalized names that had no matching candidates in the database.
