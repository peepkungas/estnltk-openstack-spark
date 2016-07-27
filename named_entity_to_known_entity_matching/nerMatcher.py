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
    x, y = ast.literal_eval(line.encode("utf-8"))
    
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
    line = line.encode("utf-8")
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
    Words shorter than 2 characters are removed.
    """
    parts = name.split(" ")
    name = ""
    for part in parts:
        if len(part) > 2:
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


def extractEntitiesMatchingTag(onePageDict,tag):
    """
    Explodes named entities which type matches the provided tag.
    Returns (entity,dictionary) pairs.
    """
    entities = onePageDict["named_entities"]
    labels = onePageDict["named_entity_labels"]
    # for multiple possibilities, create one of each
    
    entitiesWithLabels = zip(entities,labels)
    matchingEntitiesWithLabels = filter(lambda onePair : onePair[1] == tag, entitiesWithLabels)
    
    return map(lambda onePair : (onePair[0],onePageDict), matchingEntitiesWithLabels)
    

def cleanNerResult(nerResultDict):
    """
    Splits multiple possibility names (separated by "|")
    Turns each name in 'named_entities' into lowercase and removes words of length 2 or less.
    """
    named_entities = nerResultDict['named_entities']
    
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
    nerResultDict['named_entities'] = map(lambda oneEntity : cleanEntityName(oneEntity), splitNamedEntities)
    return nerResultDict

def removeDuplicatesFromNerResult(nerResultDict):
    """
    Removes duplicate named entities from 'named_entities' in nerResultDict.
    """
    named_entities = nerResultDict['named_entities']
    nerResultDict['named_entities'] = list(set(named_entities))
    return nerResultDict

def performKeyMatching(nerInputDict,knownEntityKeySet_broadcast_PER,knownEntityKeySet_broadcast_ORG):
    """
    Attempts to match each unknown named entity key to known entity keys.
    Input: 
        nerInputDict - dictionary with "named_entities" to be matched
        knownEntityKeySet_broadcast_PER - set of keys of known persons 
        knownEntityKeySet_broadcast_ORG - set of keys of known organizations
    """
    if not "named_entities" in nerInputDict.keys():
        return nerInputDict 
    if flag_ignoreNamedEntityLabels == False and not "named_entity_labels" in nerInputDict.keys():
        return nerInputDict 
    
    named_entities = nerInputDict["named_entities"]
    
    # find matches to known persons and organizations 
    # we don't assume type tags to be correct, therefore try matching to both PER and ORG for all entries
    matchingResults_PER = map(lambda oneNamedEntity : matchToKnown(oneNamedEntity,knownEntityKeySet_broadcast_PER), named_entities)
    matchingResults_ORG = map(lambda oneNamedEntity : matchToKnown(oneNamedEntity,knownEntityKeySet_broadcast_ORG), named_entities)

    # retrieve successfully matched names
    matchedNamedEntities_PER = [name[0] for name in matchingResults_PER if name[1] == True]
    matchedNamedEntities_ORG = [name[0] for name in matchingResults_ORG if name[1] == True]

    # retrieve unmatched names
    unmatchedNamedEntities_PER = [name[0] for name in matchingResults_PER if name[1] == False]
    unmatchedNamedEntities_ORG = [name[0] for name in matchingResults_ORG if name[1] == False]
    unmatchedNamedEntities = list(set(unmatchedNamedEntities_PER + unmatchedNamedEntities_ORG))
    
    outputDict = dict()
    outputDict["hostname"] = nerInputDict["hostname"]
    outputDict["path"] = nerInputDict["path"]
    outputDict["date"] = nerInputDict["date"]
    outputDict["matchedNamedEntities_PER"] = matchedNamedEntities_PER    
    outputDict["matchedNamedEntities_ORG"] = matchedNamedEntities_ORG
    outputDict["unmatchedNamedEntitites"] = unmatchedNamedEntities
    
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

def matchedPageDataToMatchedPairs(oneResult,knownEntityKeySet_broadcast_PER,knownEntityKeySet_broadcast_ORG):
    """
    Maps matchings of one page into (name,type,pagedata),[(knownEntityInfo)] pairs
    """
    knownEntityKeySet_PER = knownEntityKeySet_broadcast_PER.value
    knownEntityKeySet_ORG = knownEntityKeySet_broadcast_ORG.value
    pairs_PER = oneResult["entityMatchingPairs_PER"]
    pairs_ORG = oneResult["entityMatchingPairs_ORG"]

    pageData = (oneResult["hostname"],oneResult["path"],oneResult["date"])

    res = {}
    for onePair in pairs_PER:
        name = onePair[0]
        matchedEntityTags = onePair[1]
        matchedEntities = []
        for oneTag in matchedEntityTags:
            matchedEntityDict = knownEntityKeySet_PER[oneTag]
            knownData_PER = [matchedEntityDict[0],matchedEntityDict[1],matchedEntityDict[2],matchedEntityDict[3]]
            matchedEntities.append(knownData_PER)
        res[(name, "PER", pageData)] = matchedEntities

    for onePair in pairs_ORG:
        name = onePair[0]
        matchedEntityTags = onePair[1]
        matchedEntities = []
        for oneTag in matchedEntityTags:
            matchedEntityDict = knownEntityKeySet_ORG[oneTag]
            knownData_ORG = [matchedEntityDict[organizationIdIx],matchedEntityDict[organizationNameIx]]
            matchedEntities.append(knownData_ORG)
        res[(name, "ORG", pageData)] = matchedEntities

    return res

def countNumberOfMatches(nameMatchesPair):
    """
    Calculates the number of matches to the name
    In : (name,type,pagedata),[(knownEntityInfo)]
    Out : number of known entities
    """
    return len(nameMatchesPair[1])

def performDataRetrieval(oneResult,knownEntityDict_broadcast_PER,knownEntityDict_broadcast_ORG):
    """
    For each key in oneResult["matchedNamedEntities_PER"] and oneResult["matchedNamedEntities_ORG"],
    retrieve the matching known entity data from knownEntityDict_broadcast_PER or knownEntityDict_broadcast_ORG.
    If multiple entities match the same key, attempt to find a correct PER-ORG pair using line hashes. 
    """
    matchedNamedEntities_PER = oneResult["matchedNamedEntities_PER"]
    matchedNamedEntities_ORG = oneResult["matchedNamedEntities_ORG"]
    
    # retrieve line hashes and single possibility entity data
    hashSet_PER, matchedEntityDataList_PER, multipleMatchesList_PER = retrieveDataAndHashes(matchedNamedEntities_PER,knownEntityDict_broadcast_PER)
    hashSet_ORG, matchedEntityDataList_ORG, multipleMatchesList_ORG = retrieveDataAndHashes(matchedNamedEntities_ORG,knownEntityDict_broadcast_ORG)
   
    # cross-reference multiple possibility entities to line hashes
    crossMatchedEntityDataList_PER, uncrossMatchedEntityDataList_PER = performCrossMatching(multipleMatchesList_PER,hashSet_ORG)
    crossMatchedEntityDataList_ORG, uncrossMatchedEntityDataList_ORG = performCrossMatching(multipleMatchesList_ORG,hashSet_PER)
     
    # TODO?: if two possibilities both have a matching cross-reference, do something different?
     
    oneResult["singleMatchedEntities_PER"] = matchedEntityDataList_PER
    oneResult["crossMatchedEntities_PER"] = crossMatchedEntityDataList_PER
    oneResult["uncrossMatchedEntities_PER"] = uncrossMatchedEntityDataList_PER
    
    oneResult["singleMatchedEntities_ORG"] = matchedEntityDataList_ORG
    oneResult["crossMatchedEntities_ORG"] = crossMatchedEntityDataList_ORG
    oneResult["uncrossMatchedEntities_ORG"] = uncrossMatchedEntityDataList_ORG
    
    del oneResult["matchedNamedEntities_PER"]
    del oneResult["matchedNamedEntities_ORG"]
    
    return oneResult
            
def retrieveDataAndHashes(matchedNamedEntities,knownEntityDict_broadcast):            
    """
    For each key in matchedNamedEntities, retrieve data from knownEntityDict.
    Return set of all matched line hashes, list of data of confirmed matches, list of multi-possibility matches 
    """
    knownEntityDict = knownEntityDict_broadcast.value
    hashes = []
    namedEntityDataList = []
    multipleMatchesList = []
    
    for oneName in matchedNamedEntities:
        if len(oneName) == 1:
            oneKnownNameKey = oneName[0]
            oneKnownEntityData = list(knownEntityDict[oneKnownNameKey])
            hashes = hashes + oneKnownEntityData[0]
            namedEntityDataList.append(oneKnownEntityData[1])
        elif len(oneName) > 1:
            for oneEntity in oneName:
                oneKnownNameKey = oneName[0]
                oneKnownEntityData = list(knownEntityDict[oneKnownNameKey])
                hashes.append(oneKnownEntityData[0])
                multipleMatchesList.append(oneEntity)
    hashesSet = set(hashes)
    return hashesSet, namedEntityDataList, multipleMatchesList

def performCrossMatching(multipleMatchesList,hashSet):
    """
    Checks if multiple possibilities for matching entities can be narrowed down by PER-ORG data.
    If a common data line hash exists for multiple possibilities, choose the matching one. 
    """
    matchedNamedEntityDataList = list()
    unmatchedNamedEntityDataList = list()
    for oneMultiPossibilityEntity in multipleMatchesList:
        matchFound = False
        for oneHash in oneMultiPossibilityEntity[0]:
            if oneHash in hashSet:
                matchedNamedEntityDataList.append(oneMultiPossibilityEntity[1])
                matchFound = True
                break
        if matchFound == False:
            unmatchedNamedEntityDataList.append(oneMultiPossibilityEntity[1])

    return matchedNamedEntityDataList, unmatchedNamedEntityDataList


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
    
    flag_ignoreNamedEntityLabels = True
    
    if len(sys.argv) < 4:
        print len(sys.argv)
        print """nerMatcher.py <nerPath> <knownEntitiesPath> <outputPath>
                \n nerManifestPath - text file containing spark_estnltk NER result file paths to be processed
                \n knownEntitiesPath - CSV file with known entities
                \n outputpath - Output directory. Has to be non-existing  
                """
        sys.exit()

    startTime = time.clock()
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
    # load file with known entity data (person + organization data rows)
    knownEntitiesFile = spark.textFile(csvFilePath)

    # parse known entity file lines into tuples
    # filter out invalid lines
    # result pair : (line hash, line entity data list)
    hash_KnownEntityData_PairRDD = knownEntitiesFile.map(lambda line: parseEntities(line)).filter(lambda entry: entry is not None)

    # Extract person info from known entity lists
    # Bind each person to its data lines
    # result pair : ((per_id, per_firstname, per_lastname), [matching_line_hashes])    
    personData_MatchingHashes_PairRDD = hash_KnownEntityData_PairRDD.map(lambda entity: (extractPerFromEntity(entity[1]), entity[0]) ).groupByKey()

    # clean name for PER matching
    # result pair : ("firstname lastname", ([ [matching_line_hashes],(per_id,per_firstname,per_lastname) ) ])
    matchableName_HashAndData_PairRDD_PER = personData_MatchingHashes_PairRDD.map(lambda entity : (cleanEntityName("{} {}".format(entity[0][1].strip('"'),entity[0][2].strip('"'))), (entity[1], entity[0]) ) )
    knownEntityKeySet_broadcast_PER = spark.broadcast(set(matchableName_HashAndData_PairRDD_PER.groupByKey().keys().collect()))
    knownEntityDict_broadcast_PER = spark.broadcast(dict(matchableName_HashAndData_PairRDD_PER.groupByKey().collect()))
    
    # Extract organization info from known entity rows
    # Bind each organization to its data lines
    # result pair : ((org_id, org_name), [matching_line_hashes]) 
    orgData_MatchingHashes_PairRDD = hash_KnownEntityData_PairRDD.map(lambda entity: (extractOrgFromEntity(entity[1]), entity[0]) ).groupByKey()

    # clean name for ORG matching
    # result pair : ("orgname", ([ [matching_line_hashes],(org_id,org_name) ) ])
    matchableName_HashAndData_PairRDD_ORG = orgData_MatchingHashes_PairRDD.map(lambda entity : (cleanEntityName(entity[0][1].strip('"')), ( entity[1], entity[0] ) ))
    knownEntityKeySet_broadcast_ORG = spark.broadcast(set(matchableName_HashAndData_PairRDD_ORG.groupByKey().keys().collect()))
    knownEntityDict_broadcast_ORG = spark.broadcast(dict(matchableName_HashAndData_PairRDD_ORG.groupByKey().collect()))


    ## DEBUG
    matchableName_HashAndData_PairRDD_ORG.groupByKey().saveAsTextFile(outputPath+"/data_ORG")

    
    ################
    # Parse manifest of unknown named entity input files
    ################
    inputPathList = []
    manifestFile = open(manifestPath,'r')
    for line in manifestFile:
        inputPathList.append(line)

    ################
    # Process unknown named entities to matchable names
    ################
    i=0
    for inputPath in inputPathList:
        i += 1
        print("Processing unknown named entity input file: " + inputPath)
        # load file with unknown named entities (NLTK results file)
        try:
            nerResultsFile = spark.textFile(inputPath)
        except:
            print("ERR: unable to open " + inputPath)
            continue
        # extract NER results from file
        # each result element: dictionary with ['named_entities'],['hostname'],['path'],['date']
        # filter out invalid (empty) results
        nerResultDictRDD = nerResultsFile.map(lambda line: parseData(line)).filter(lambda entry: entry is not None)
    
        # clean names
        # split if multiple possibilities ("|" separator in name)
        cleanNerResultDictRDD = nerResultDictRDD.map(lambda nerResult : cleanNerResult(nerResult))
        # remove duplicates
        nerResultsRDD = cleanNerResultDictRDD.map(lambda nerResult : removeDuplicatesFromNerResult(nerResult))
        
        # DEBUG
        nerResultsRDD.saveAsTextFile(outputPath + str(i) + "/cleaned_ner")
    
    
    
        ################
        # Matching : match each name in each NER result dict to known entities
        ################
        print("Matching unknown names to known entities...")
        # Returns : NER result dicts with added ['matchedNamedEntities_PER'],['matchedNamedEntities_ORG'],['unmatchedNamedEntities']
        matchedNerResultsRDD = nerResultsRDD.map(lambda oneResult : performKeyMatching(oneResult,knownEntityKeySet_broadcast_PER,knownEntityKeySet_broadcast_ORG))
        
        # DEBUG
        matchedNerResultsRDD.saveAsTextFile(outputPath+ str(i) + "/matching")
    
    
        ################
        # Data retrieval : 
        #  if one match, retrieve
        #  if multiple matches, search for PER-ORG line hashes that match 
        ################
        print("Retrieving matched entity data...")
        entityDataRDD = matchedNerResultsRDD.map(lambda oneResult : performDataRetrieval(oneResult,knownEntityDict_broadcast_PER,knownEntityDict_broadcast_ORG))
        
        # Save result
        entityDataRDD.saveAsTextFile(outputPath+ str(i) + "/entityData")

        print("Finished file: " + inputPath)
    
    spark.stop()
    endTime = time.clock()
    print "Time to finish:" + str(endTime - startTime)
