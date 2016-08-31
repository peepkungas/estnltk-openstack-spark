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