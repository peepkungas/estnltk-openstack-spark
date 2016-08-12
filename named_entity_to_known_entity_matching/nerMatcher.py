#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
NER result to database matching
input:
  NER values
  file with person and organization data
"""
"""
procedure:
 get NER values
 get name data
 for each:
   ignore len 2 and less
   match to known brands (Google <=> Google Co.)
   match to name data
   Heuristic 1: There is a single match for a recognized name 
        in the list of companies and persons. In this case use the 
        single match.
   Heuristic 2a: There are multiple matches for a recognized name 
        in the list of persons, but together with a company name 
        from the same URL there is only single match. In this case 
        use the single match.
   Heuristic 2b: There are multiple matches for a recognized name 
       in the list of companies, but together with a person name from 
       the same URL there is only single match. In this case use 
       the single match.
   Heuristic 3a: There are multiple matches for a recognized name 
       in the list of persons together with a company name from 
       the same URL but there is only single match between the 
       membership period and the content date. In this case use 
       the single match.
   Heuristic 3b: There are multiple matches for a recognized name 
       in the list of companies together with a person name from the 
       same URL but there is only single match between the membership 
       period and the content date. In this case use the single match.
   Heuristic 4: In addition to preceding use former names of companies 
       and persons
"""
from pyspark import SparkContext, SparkConf
from pyspark.shuffle import InMemoryMerger
from subprocess import call
import sys
from operator import add
import re
import justext
import urllib2
import json
import estnltk
import random
import ast
import time
import re
import hashlib
import sets
import os

def parseData(line):
    """
    Parses NER results from text line.
    """
    if len(line)< 10:
        return None
    x, y = ast.literal_eval(line.encode("raw_unicode_escape","replace"))
    
    # x - hostname::path::date
    # y - dictionary
    # y['named_entity_labels'] - list of labels
    # y['named_entities'] - list if entity strings
    
    # when y - dictionary, is empty it contains "NO_TEXT", this nerResults are filtered out 
    if y == "NO_TEXT":
        return None
    # if the dictionary does not have named entities, return nothing
    
    if flag_ignoreNamedEntityLabels == True and 'named_entity_labels' in y.keys():
        del y['named_entity_labels']
    
    # extract from shape {ner:{named_entities:{},named_entity_labels:{}}}
    if not 'named_entities' in y.keys():
        if ('ner' in y.keys()):
            y["named_entities"] = y['ner']['named_entities']
    if flag_ignoreNamedEntityLabels == False and not 'named_entity_labels' in y.keys():
        if ('ner' in y.keys()):
            y["named_entity_labels"] = y['ner']['named_entity_labels']
                        
    keyparts = x.split("::")
    y['hostname'] = keyparts[0]
    y['path'] = keyparts[1]
    y['date'] = keyparts[2]
    return y

def parseEntities(line):
    """
    Parses known entities from text line to tuple.
    """
    line = line.encode("raw_unicode_escape","replace")
    # split values by comma which represents separator and assign them into a tuple.
    values = line.split(",")
    # discard lines which contain less than nine columns
    if(len(values) != 9 or "," in values[0] or values[1]== '""' or values[2] == '""' or values[5] == '""'):
        return None

    # add hash of line to uniquely identify line data
    return ( hashlib.md5(line).digest(),values )
    
def cleanEntityName(name):
    """
    Cleans a name by stripping whitespaces and transforming it into lowercase.
    Certain words of length 2 or less are removed.
    """
    name = name.strip("'")
    name = name.strip('"')
    parts = name.split(" ")
    name = ""
    for part in parts:
        if len (part) > 2:
        #if part not in filteredWords:
            name = name + " " + part
    name = name.lower().strip()
    return name

def extractPerFromEntity(entity):
    """
    Returns person data from known entity row.
    """
    return (entity[personIdIx],entity[firstNameIx],entity[surnameIx])

def extractOrgFromEntity(entity):
    """
    Returns organization data from known entity row.
    """
    return (entity[organizationIdIx],entity[organizationNameIx])  

def cleanNerResult(nerResultDict):
    """
    Splits multiple possibility names (separated by "|")
    Turns each name in 'named_entities' into lowercase and filters out words of length 2 or less.
    """
    named_entities = nerResultDict['named_entities']
    named_entities = list(set(named_entities))
    splitNamedEntities = []
    for oneNameBlock in named_entities:
        nameAlternatives = []
        spaceSplitParts = oneNameBlock.split(" ")
        for oneSpaceSplitPart in spaceSplitParts:
            pipeSplitParts = oneSpaceSplitPart.split("|")
            if len(nameAlternatives) == 0:
                nameAlternatives = pipeSplitParts
            else:     
                newNameAlternatives = []
                for onePipeSplitPart in pipeSplitParts:
                    for oneAlternativePart in nameAlternatives:
                        newNameAlternatives.append(oneAlternativePart + " " + onePipeSplitPart)
                nameAlternatives = newNameAlternatives
        splitNamedEntities += nameAlternatives
            
    # turn lowercase and remove words of len 2 or less
    cleanedEntityNames = map(lambda oneEntity : cleanEntityName(oneEntity), splitNamedEntities)
    nerResultDict["alternative_named_entities"] = splitNamedEntities
    nerResultDict["named_entity_keys"] = filter(lambda name : len(name) > 2, cleanedEntityNames)
    return nerResultDict

def removeDuplicatesFromNerResult(nerResultDict):
    """
    Removes duplicate named entities from "named_entity_keys" in nerResultDict.
    """
    named_entities = nerResultDict["named_entity_keys"]
    nerResultDict["named_entity_keys"] = list(set(named_entities))
    return nerResultDict

def performKeyMatching(nerInputDict,knownEntityKeySet_broadcast_PER,knownEntityKeySet_broadcast_ORG):
    """
    Attempts to match each unknown named entity key to known entity keys.
    Input: 
        nerInputDict - dictionary with "named_entity_keys" to be matched
        knownEntityKeySet_broadcast_PER - set of keys of known persons 
        knownEntityKeySet_broadcast_ORG - set of keys of known organizations
    """
    if not "named_entity_keys" in nerInputDict.keys():
        return nerInputDict 
    if flag_ignoreNamedEntityLabels == False and not "named_entity_labels" in nerInputDict.keys():
        return nerInputDict 
    
    named_entity_keys = nerInputDict["named_entity_keys"]
    
    # find matches to known persons and organizations 
    # we don't assume type tags to be correct, therefore try matching to both PER and ORG for all entries
    matchingResults_PER = map(lambda oneNamedEntity : matchToKnown(oneNamedEntity,knownEntityKeySet_broadcast_PER), named_entity_keys)
    matchingResults_ORG = map(lambda oneNamedEntity : matchToKnown(oneNamedEntity,knownEntityKeySet_broadcast_ORG), named_entity_keys)

    # retrieve successfully matched names
    matchedNamedEntityKeys_PER = [name[0] for name in matchingResults_PER if name[1] == True]
    matchedNamedEntityKeys_ORG = [name[0] for name in matchingResults_ORG if name[1] == True]

    # retrieve unmatched names
    unmatchedNamedEntities_PER = [name[0] for name in matchingResults_PER if name[1] == False]
    unmatchedNamedEntities_ORG = [name[0] for name in matchingResults_ORG if name[1] == False]
    unmatchedNamedEntityKeys = list( set.intersection(set(unmatchedNamedEntities_PER),set(unmatchedNamedEntities_ORG)) ) 
    outputDict = dict()
    outputDict["hostname"] = nerInputDict["hostname"]
    outputDict["path"] = nerInputDict["path"]
    outputDict["date"] = nerInputDict["date"]
    outputDict["alternative_named_entities"] = nerInputDict["alternative_named_entities"]
    outputDict["matchedNamedEntityKeys_PER"] = matchedNamedEntityKeys_PER    
    outputDict["matchedNamedEntityKeys_ORG"] = matchedNamedEntityKeys_ORG
    outputDict["unmatchedNamedEntityKeys"] = unmatchedNamedEntityKeys
    return outputDict
    
    
def matchToKnown(oneNamedEntity, knownNamesColl_broadcast):
    """
    Matches a name to a collection of known names.
    Returns (name,True) if successful, (name,False) otherwise.
    """
    knownNamesColl = knownNamesColl_broadcast.value
    if oneNamedEntity in knownNamesColl:
        return (oneNamedEntity,True)
    else:
        return (oneNamedEntity,False)

def countNumberOfMatches(nameMatchesPair):
    """
    Calculates the number of matches to the name
    In : (name,type,pagedata),[(knownEntityInfo)]
    Out : number of known entities
    """
    return len(nameMatchesPair[1])

def performDataRetrieval(oneResult,knownEntityDict_broadcast_PER,knownEntityDict_broadcast_ORG):
    """
    For each key in oneResult["matchedNamedEntityKeys_PER"] and oneResult["matchedNamedEntityKeys_ORG"],
    retrieve the matching known entity data from knownEntityDict_broadcast_PER or knownEntityDict_broadcast_ORG.
    If multiple entities match the same key, attempt to find a correct PER-ORG pair using line hashes. 
    """
    matchedNamedEntityKeys_PER = oneResult["matchedNamedEntityKeys_PER"]
    matchedNamedEntityKeys_ORG = oneResult["matchedNamedEntityKeys_ORG"]
    
    # retrieve line hashes and single possibility entity data
    hashSet_PER, singleMatchKeyEntityDataList_PER, multipleMatchesKeyHashAndEntityDataPairList_PER = retrieveDataAndHashes(matchedNamedEntityKeys_PER,knownEntityDict_broadcast_PER)
    hashSet_ORG, singleMatchKeyEntityDataList_ORG, multipleMatchesKeyHashAndEntityDataPairList_ORG = retrieveDataAndHashes(matchedNamedEntityKeys_ORG,knownEntityDict_broadcast_ORG)
   
    # cross-reference multiple possibility entities to line hashes
    crossmatchedKeyEntityDataList_PER, uncrossmatchedKeyEntityCountList_PER = performCrossMatching(multipleMatchesKeyHashAndEntityDataPairList_PER,hashSet_ORG)
    crossmatchedKeyEntityDataList_ORG, uncrossmatchedKeyEntityCountList_ORG = performCrossMatching(multipleMatchesKeyHashAndEntityDataPairList_ORG,hashSet_PER)
     
    #TODO?: if two possibilities both have a matching cross-reference, do something different?
     
    oneResult["singleMatchedEntities_PER"] = singleMatchKeyEntityDataList_PER
    oneResult["crossMatchedEntities_PER"] = crossmatchedKeyEntityDataList_PER
    if flag_doUncrossmatchedEntityDataOutput:
        oneResult["uncrossMatchedEntities_PER"] = uncrossmatchedKeyEntityCountList_PER
    
    oneResult["singleMatchedEntities_ORG"] = singleMatchKeyEntityDataList_ORG
    oneResult["crossMatchedEntities_ORG"] = crossmatchedKeyEntityDataList_ORG
    if flag_doUncrossmatchedEntityDataOutput:
        oneResult["uncrossMatchedEntities_ORG"] = uncrossmatchedKeyEntityCountList_ORG
    
    del oneResult["matchedNamedEntityKeys_PER"]
    del oneResult["matchedNamedEntityKeys_ORG"]
    
    return oneResult
            
def retrieveDataAndHashes(matchedNamedEntityKeys,knownEntityDict_broadcast):
    """
    For each name key in matchedNamedEntitityKeys, retrieve the corresponding entity data from knownEntityDict_broadcast.
    Returns set of all hashes, entity data of single-matches, hash-and-entity-data of multi-matches.
    """            
    knownEntityDict = knownEntityDict_broadcast.value
    allHashes = []
    singleMatchKeyEntityDataList = []
    multipleMatchesKeyHashAndEntityDataList = []
    
    for oneKnownNameKey in matchedNamedEntityKeys:
        knownEntityPairsList = list(knownEntityDict[oneKnownNameKey]) ## list of [([hashes], (name,id) )] pairs
        if len(knownEntityPairsList) == 1: ## A single known entity matches the key
            for oneKnownEntityDataPair in knownEntityPairsList:  
                hashes = list(oneKnownEntityDataPair[0])
                allHashes = allHashes + list(hashes)
                keyAndEntityDataPair = (oneKnownNameKey, oneKnownEntityDataPair[1])
                singleMatchKeyEntityDataList.append(keyAndEntityDataPair)
        elif len(knownEntityPairsList) > 1: ## Multiple known entities match the key
            hashesAndEntityDataPairList = []
            for oneKnownEntityDataPair in knownEntityPairsList:  
                hashes = list(oneKnownEntityDataPair[0])
                allHashes = allHashes + list(hashes)
                keyAndHashesAndEntityDataPair = (oneKnownNameKey, oneKnownEntityDataPair) 
                hashesAndEntityDataPairList.append(oneKnownEntityDataPair) 
            multipleMatchesKeyHashAndEntityDataList.append( (oneKnownNameKey, hashesAndEntityDataPairList) ) ## (key, [([hashes], (id, name)]))

    hashesSet = set(allHashes)
    return hashesSet, singleMatchKeyEntityDataList, multipleMatchesKeyHashAndEntityDataList

def performCrossMatching(multipleMatchesHashAndEntityDataPairList,otherTypeHashSet):
    """
    Checks if multiple possibilities for matching entities can be narrowed down by PER-ORG data.
    If a common hash exists between an ORG entity and any entity from PER, retrieve it.
    If multiple matches are found, then unable to narrow down, add to unmatched entity list?
    """
    matchedNamedEntityKeyDataList = []
    unmatchedNamedEntityKeyDataList = []
    for oneKeyHashAndEntityDataTuple in multipleMatchesHashAndEntityDataPairList: ## (key, [([hashes], (id, name)]))
        oneKey = oneKeyHashAndEntityDataTuple[0]
        oneHashAndEntityDataPairList = oneKeyHashAndEntityDataTuple[1]
        matchFound = False
        for oneHashAndEntityDataPair in oneHashAndEntityDataPairList: ## Cross-reference entity candidate hashes to other type hashes
            hashes = list(oneHashAndEntityDataPair[0])
            for oneHash in hashes:
                if oneHash in otherTypeHashSet:
                    matchFound = True
                    break
            if matchFound == True: ## Successful cross-matching, ignore others?
                entityData = oneHashAndEntityDataPair[1]
                matchedNamedEntityKeyDataList.append( (oneKey,entityData) )
                break
        if matchFound == False: ## Unsuccessful cross-matching
            unmatchedNamedEntityKeyDataList.append( (oneKey,len(oneHashAndEntityDataPairList)) )
    return matchedNamedEntityKeyDataList, unmatchedNamedEntityKeyDataList


if __name__ == "__main__":
    """
    Executes NER and known entity matching.
    """
    personIdIx = 0
    firstNameIx = 1
    surnameIx = 2
    organizationIdIx = 4
    organizationNameIx = 5
    hashIx = 10
    
    flag_doAllKnownEntityKeysOutput = False ## if True, output the lists of all known entity keys to data_PER and data_ORG directories 
    flag_ignoreNamedEntityLabels = True ## if True, ignore PER and ORG tags found from NLTK
    flag_doUncrossmatchedEntityDataOutput = True ## if True, output key and candidate count for each unsuccessful cross-matching
    flag_doMatchingOutput = False ## If True, output matched/unmatched keys to "/matching" directory
    #TODO: improve list of filtered words in names
    filteredWords = ["as","kü","oü"]
    
    
    if len(sys.argv) < 4:
        print len(sys.argv)
        print """nerMatcher.py <nerPath> <knownEntitiesPath> <outputPath>
                \n nerManifestPath - text file containing spark_estnltk NER result file paths to be processed
                \n knownEntitiesPath - CSV file with known entities
                \n outputpath - Output directory. Has to be non-existing  
                """
        sys.exit()

    startTime = time.time()
    manifestPath = sys.argv[1]
    csvFilePath = sys.argv[2]
    outputPath = sys.argv[3]

    # Ensure that the output directory does not exist
    print("OUT: " + outputPath)
    if outputPath.startswith("hdfs:"):
        if call(["hadoop", "fs", "-test", "-d",outputPath]) == 0:
            print("Output directory already exists. Stopping.")
            sys.exit("Output directory already exists.")     
    elif os.path.isdir(outputPath):
        print("Output directory already exists. Stopping.")
        sys.exit("Output directory already exists.")    
        
    conf = SparkConf()

    spark = SparkContext(appName="NER_matcher", conf=conf)

    ################
    #  Process known entity data into matchable names
    ################
    print("Processing known entity data csv file...")
    knownEntityProcessingStartTime = time.time()
    # load file with known entity data (person + organization data rows)
    knownEntitiesFile = spark.textFile(csvFilePath,use_unicode=True)

    # parse known entity file lines into tuples
    # filter out invalid lines
    # result pair : (line hash, line entity data list)
    hash_KnownEntityData_PairRDD = knownEntitiesFile.map(lambda line: parseEntities(line)).filter(lambda entry: entry is not None)

    # Extract person info from known entity lists
    # Group line hashes by PER data
    # result pair : ((per_id, per_firstname, per_lastname), [matching_line_hashes])    
    personData_MatchingHashes_PairRDD = hash_KnownEntityData_PairRDD.map(lambda entity: (extractPerFromEntity(entity[1]), entity[0]) ).groupByKey()

    # clean name for PER matching (lowercase with len <3 words removed)
    # result pair : ("firstname lastname", ([ [matching_line_hashes],(per_id,per_firstname,per_lastname) ) ])
    matchableName_HashAndData_PairRDD_PER = personData_MatchingHashes_PairRDD.map(lambda entity : (cleanEntityName("{} {}".format(entity[0][1],entity[0][2])), (entity[1], entity[0]) ) )
    knownEntityKeySet_broadcast_PER = spark.broadcast(set(matchableName_HashAndData_PairRDD_PER.groupByKey().keys().collect()))
    knownEntityDict_broadcast_PER = spark.broadcast(dict(matchableName_HashAndData_PairRDD_PER.groupByKey().collect()))

    # Extract organization info from known entity rows
    # Group line hashes by ORG data
    # result pair : ((org_id, org_name), [matching_line_hashes]) 
    orgData_MatchingHashes_PairRDD = hash_KnownEntityData_PairRDD.map(lambda entity: (extractOrgFromEntity(entity[1]), entity[0]) ).groupByKey()

    # clean name for ORG matching (lowercase with len <3 words removed)
    # result pair : ("orgname", ([ [matching_line_hashes],(org_id,org_name) ) ])
    matchableName_HashAndData_PairRDD_ORG = orgData_MatchingHashes_PairRDD.map(lambda entity : (cleanEntityName(entity[0][1]), ( entity[1], entity[0] ) )).filter(lambda keyPair: len(keyPair[0])>0 )
    knownEntityKeySet_broadcast_ORG = spark.broadcast(set(matchableName_HashAndData_PairRDD_ORG.groupByKey().keys().collect()))
    knownEntityDict_broadcast_ORG = spark.broadcast(matchableName_HashAndData_PairRDD_ORG.groupByKey().collectAsMap())

    if flag_doAllKnownEntityKeysOutput: ## if true, output all matchable keys
        matchableName_HashAndData_PairRDD_PER.groupByKey().keys().saveAsTextFile(outputPath+"/data_PER")
        matchableName_HashAndData_PairRDD_ORG.groupByKey().keys().saveAsTextFile(outputPath+"/data_ORG")

    knownEntityProcessingEndTime = time.time()
    print("Known entity file processed in: " + str(knownEntityProcessingEndTime-knownEntityProcessingStartTime))
    
    ################
    # Parse manifest of unknown named entity input files
    ################
    inputPathString = ""
    manifestFile = open(manifestPath,'r')
    for line in manifestFile:
        inputPathString = inputPathString + line.strip() +","
    inputPathString = inputPathString.strip(",")
    ################
    # Process unknown named entities to matchable names
    ################
    print("Processing unknown-named-entity input files...")
    oneFileStartTime = time.time()
    # extract NER results from file
    nerResultsFile = spark.textFile(inputPathString,use_unicode=True)
    # each result element: dictionary with ['named_entities'],['hostname'],['path'],['date']
    # filter out invalid (empty) results
    nerResultDictRDD = nerResultsFile.map(lambda line: parseData(line)).filter(lambda entry: entry is not None)
    # clean names
    # split if multiple possibilities ("|" separator in name)
    cleanNerResultDictRDD = nerResultDictRDD.map(lambda nerResult : cleanNerResult(nerResult))
    # remove duplicates
    nerResultsRDD = cleanNerResultDictRDD.map(lambda nerResult : removeDuplicatesFromNerResult(nerResult))

    ################
    # Matching : match each name in each NER result dict to known entities
    ################
    print("Matching unknown names to known entities...")
    # Returns : NER result dicts with added ['matchedNamedEntities_PER'],['matchedNamedEntities_ORG'],['unmatchedNamedEntities']
    matchedNerResultsRDD = nerResultsRDD.map(lambda oneResult : performKeyMatching(oneResult,knownEntityKeySet_broadcast_PER,knownEntityKeySet_broadcast_ORG))
    nerResultsRDD.unpersist()
    
    if flag_doMatchingOutput: ## Outputs matching results (matching and unmatching keys)
        matchedNerResultsRDD.saveAsTextFile(outputPath + "/matching")


    ################
    # Data retrieval : 
    #  if one match, retrieve
    #  if multiple matches, search for PER-ORG line hashes that match 
    ################
    print("Retrieving matched entity data...")
    entityDataRDD = matchedNerResultsRDD.map(lambda oneResult : performDataRetrieval(oneResult,knownEntityDict_broadcast_PER,knownEntityDict_broadcast_ORG))
    matchedNerResultsRDD.unpersist()
    
    # Save result
    entityDataRDD.saveAsTextFile(outputPath + "/entityData")

    print("Finished unknown entity files.")
    oneFileEndTime = time.time()
    print("Time taken: " + str(oneFileEndTime - oneFileStartTime))
    entityDataRDD.unpersist()
    spark.stop()
    endTime = time.time()
    print "Time to finish:" + str(endTime - startTime)
