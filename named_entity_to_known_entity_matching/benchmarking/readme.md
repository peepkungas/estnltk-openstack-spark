# Benchmarking properties
The benchmark is created by manually processing the names detected from one input file. For each name, there are two properties that the name has: the human expectation and the actual result.

## Human expectation
	* EHM: Expected to be matchable. A human would expect this name to have a singular matching person or organization (e.g "Tartu Ülikool").
	* NEHM: Not expected to be matchable. A human would not expect to be able to narrow down this name to a single name or organization (e.g "Bob").
	* NAPO: Not a person or organization. A human would consider it to be an entity, but one that is not a person or organization (e.g countries, locations).
	* NAN : Not a name. A human would not consider it to be a name of a person or organization and expects it to be a fault in input data (e.g "OK").

## Matching result
	* 0 matches : There are no matching entities in the database, including partial matches.
	* 1 match : There is exactly one matching entity in the database that matches this name.
	* 2+ matches : There are multiple entitites in the database that match this name.

# Layout of benchmark input file
The benchmark file is a work in process.
	* DOMAIN : domain name
	* PATH : path in domain
	* DATE : timestamp of page collection
	* NAMED_ENTITY : name detected by ESTNLTK
	* TAG : describes type of name. 
	** o - organization
	** p - person
	** nan - not a name
	** nop - not organization or person, other type name (e.g location)
	** empty if unknown type
	* TAG2 : describes properties of this name. 
	** faulty - contains errors, but may be understandable to a human
	** multi - multiple entities match this name (name is too general to narrow down)
	** famous - well-known person or international organization. 
	** ? - yet-unknown type or unknown matching entities. Caused by having multiple unusual matches or a lack of information.
	* CLEANED_NAME : If TAG2=faulty, then the actual name when possible. If TAG2=?, then the name of most likely matching entity.
	* WIKI_LINK : If TAG2=famous, link to the Wiki article of that name when available.
	* REGISTRY_CODE_OR_BIRTHDATE_FROM_INFOREGISTER : If TAG=o, registration code of organization. If TAG=p, birthdate of person. If TAG2=multi, then this is a birthdate only if there is exactly one matching person and one or more matching organizations.
	* ALTERNATE_POSSIBILITIES : (optional) script-collected data from Inforegister to assist data entry. Is not complete, has many missing fields.
	* ALT_COUNT : (optional) count of possible matching entities. If it equals one, we can automate regcode/birthdate extraction
	* SINGLE_ALT_REG_VALUE : (optional) If ALT_COUNT=1, contains the auto-extracted regcode/birthdate