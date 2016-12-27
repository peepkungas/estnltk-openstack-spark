#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
NER result to database matching
input:
  NER values
  file with person and organization data
"""
from pyspark.storagelevel import StorageLevel
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
import getopt
import estnltk

class NamedEntity():
    name = u""
    keys = []
    allowMatchingAsPER = True
    def __str__(self):
        return "({0},{1})".format(self.name,self.keys)
    def __unicode__(self):
        return u"({0},{1})".format(self.name,self.keys)

def parseData(line):
    """
    Parses NER results from text line in format (x,y) where:
    # x - string in format 'hostname::path::date' or 'protocol::hostname:path::params::date'
    # y - dictionary containing the following:
    # y['named_entities'] - list of named entity strings
    # y['named_entity_labels'] - list of named entity type labels (optional)
    """
    if len(line)< 10:
        return None
#    x, y = ast.literal_eval(line.encode("raw_unicode_escape","replace"))
    x, y = ast.literal_eval(line.decode(inputEncoding))
    # when dictionary y is empty it contains "NO_TEXT", filter these results 
    if y == "NO_TEXT":
        return None
    
    # extract from shape {ner:{named_entities:{},named_entity_labels:{}}}
    if not 'named_entities' in y.keys():
        if ('ner' in y.keys()):
            y["named_entities"] = y['ner']['named_entities']
    if not 'named_entity_labels' in y.keys():
        if ('ner' in y.keys()):
            y["named_entity_labels"] = y['ner']['named_entity_labels']

    # If enabled, preserve name and label pairs for output
    if flag_doLabelPairOutput:
        y["nameLabelPairs"] = zip(y["named_entities"], y["named_entity_labels"])
    
    # Named entity labels are often incorrect, so we ignore them in matching
    if flag_ignoreNamedEntityLabels == True and 'named_entity_labels' in y.keys():
        del y['named_entity_labels']
    
    # if enabled, remove names that are subnames of other names on the same page
    if flag_removeSubnames:
        sortedNames = sorted(y["named_entities"],key=unicode.upper)
        filteredNames = []
        for i in range(len(sortedNames)):
            oneName = sortedNames[i]
            tail = sortedNames[i+1:]
            isSubName = False
            for otherName in tail:
                if oneName.upper() in otherName.upper():
                    isSubName = True
                    break
            if not isSubName:
                filteredNames.append(oneName)
        y["named_entities"] = filteredNames
    
    y["pageId"] = x
    return y

def parseEntities(line):
    """
    Parses known entities from text line to tuple.
    Returns (lineHash,lineDict) pair,
    where lineHash is MD5 hash of row(unique ID), 
    where lineDict contains "id_PER","firstName_PER","lastName_PER","id_ORG","name_ORG" (etc).
    """
    """
    input line formats:
    "EE","00000000000","Isiku","Nimi","0000-00-00",\N,\N,\N,\N,\N
    "EE","KRDXUNI-000000","Isiku","nimi","0000-00-00","Registrikood","Nimi","0000-00-00","0000-00-00","contact"
    "KRDXUNI-000000","Isiku","nimi","0000-00-00","Registrikood",\N,"0000-00-00","0000-00-00","contact"
    """
    
    #line = line.encode("raw_unicode_escape","replace") 
    #line = line.decode("raw_unicode_escape")
    # split values by comma which represents separator and assign them into a tuple.
    values = line.split(",")
    values = [value.strip("'") for value in values]
    values = [value.strip('"') for value in values]
    returnDict = dict()
    if len(values) == 10:
        # tag,id_per,firstname,lastname,birthdate,id_org,name_org,startdate,enddate,relation
        returnDict["tag"] = values[0]
        returnDict["id_PER"] = values[1]
        returnDict["firstName_PER"] = values[2]
        returnDict["lastName_PER"] = values[3]
        returnDict["birthDate_PER"] = cleanDate(values[4])
        returnDict["id_ORG"] = values[5]
        returnDict["name_ORG"] = values[6]
        returnDict["startDate"] = cleanDate(values[7])
        returnDict["endDate"] = cleanDate(values[8])
        returnDict["relation"] = values[9]
    elif len(values) == 9:
        # id_per,firstname,lastname,birthdate,id_org,name_org,startdate,enddate,relation
        returnDict["id_PER"] = values[0]
        returnDict["firstName_PER"] = values[1]
        returnDict["lastName_PER"] = values[2]
        returnDict["birthDate_PER"] = cleanDate(values[3])
        returnDict["id_ORG"] = values[4]
        returnDict["name_ORG"] = values[5]
        returnDict["startDate"] = cleanDate(values[6])
        returnDict["endDate"] = cleanDate(values[7])
        returnDict["relation"] = values[8]
    else:
        # discard other lines
        return None

    # add hash of line to uniquely identify line data
    line = line.encode(inputEncoding) 
    return ( hashlib.md5(line).digest(), returnDict )
    
def cleanDate(dateString):
    """
    Returns None if input is "0000-00-00", unchanged input string otherwise.
    """
    if (dateString == "0000-00-00"):
        return None
    else:
        return dateString
    
def processExcludeRow(oneRow):
    """
    Process exclude file row.
    Returns dictionary with "name","tag1","synonym","tag2" keys.
    """
    if len(oneRow) == 0:
        return None
    #oneRow = oneRow.decode(inputEncoding)
    
    parts = oneRow.split(",")
    rowDict = {}
    try:
        rowDict["name"] = parts[0]
        rowDict["tag1"] = parts[1]
        rowDict["synonym"] = parts[2]
        rowDict["tag2"] = parts[3]
    except:
        pass
    if len(rowDict.keys()) == 0:
        return None
    else:
        return rowDict

def normalizeName(name, removeShortWords=True, doSynonymReplace=True):
    """
    Cleans a name by stripping whitespaces and transforming it into lowercase. 
    If removeShortWords==True, words of length 2 or less are removed. Returns None if no words remain, normalized name otherwise.
    If doSynonymReplace==True, use orgtypeReplaceDict to replace certain words with short-form synonyms.
    """
    if doSynonymReplace:
        synonymDict = synonymDict_broadcast.value
    name = name.strip("'")
    name = name.strip('"')
    parts = name.split(" ")
    name = ""
    replacedSynonym = ""
    for part in parts:
        part = part.lower()
        if doSynonymReplace == True:
            if part in orgtypeReplaceDict.keys():
                part = orgtypeReplaceDict[part]
                replacedSynonym = part
                continue
            if part in orgtypeReplaceDict.values():
                replacedSynonym = part
                continue
            if part in synonymDict:
                #part = synonymDict[part]
                pass
        if len (part) > 2 or removeShortWords==False:
            part = part.strip("'")
            part = part.strip('"')
            name = name + " " + part
    if replacedSynonym != "":
        name = replacedSynonym.lower() + " " + name.strip()
    name = name.lower().strip()
    if len(name)<=2 and removeShortWords==True:
        return None
    return name
    
def normalizeNamedEntityKeys(namedEntityKeys):
    """
    Applies name normalization to each key in input key list. 
    Returns (name,[normalized_keys]).
    """
    keys = namedEntityKeys
    normalizedKeys = [normalizeName(oneKey, removeShortWords=False) for oneKey in keys]
    normalizedKeys = filter(lambda oneKey: oneKey is not None, normalizedKeys)
    normalizedKeys = list(set(normalizedKeys))
    return normalizedKeys

def lemmatizeAndPairWithData(entity,type="org"):
    """
    Lemmatizes name of entity in form ((id,name),[hashes]). Specify type as "per" or "org" for proper handling.
    Returns list of entities with alternative names.
    """
    if (type == "org"):
        name = entity[0][1]
        lemmatizedNames = lemmatizeName(name)
        # Also preserve original
        lemmatizedNames.append(name)
        lemmatizedNames = [normalizeName(name, removeShortWords=False) for name in lemmatizedNames]
        # remove duplicates after normalization
        lemmatizedNames = list(set(lemmatizedNames))
        nameDataPairs = [((entity[0][0],oneName),entity[1]) for oneName in lemmatizedNames]
    #NOTE: PER handling is not implemented
    return nameDataPairs    
    
def lemmatizeName(name):
    """
    Applies NLTK lemmatization to known entity names. Returns a list of name alternatives.
    """
    lemmaList = estnltk.Text(name).lemmas
    lemmaList = explodeStringIntoAlternateForms(" ".join(lemmaList))
    # If any lemmatized name is shorter than multiplier*len(name), they are name parts.
#     if len(lemmaList) > 1:
#         for part in lemmaList:
#             if len(part) < lemmatizedNamePartMultiplier * len(name):
#                 # At least one word is not a proper alternative name, therefore none are. Concatenate them.
#                 outName = " ".join(lemmaList)
#                 # If too many words have been omitted (the result is too short), ignore the result
#                 if len(outName) < lemmatizedNamePartMultiplier * len(name):
#                     return []
#                 else:
#                     return [outName]
#     # Otherwise, each lemmatized name is a proper alternative. Return them.
    return lemmaList
    

def extractPerFromEntityLineDict(entityDict):
    """
    Returns person data from known entity row dictionary.
    Returns (id_PER,firstName_PER,lastName_PER) tuple.
    """
    return (entityDict["id_PER"],entityDict["firstName_PER"],entityDict["lastName_PER"])

def extractOrgFromEntityLineDict(entityDict):
    """
    Returns organization data from known entity row dictionary.
    If PER name exactly matches ORG name, return None.
    Otherwise return (id_ORG,name_ORG) tuple.
    """
    name_PER = entityDict["firstName_PER"] + " " + entityDict["lastName_PER"]
    name_PER = normalizeName(name_PER, removeShortWords=False, doSynonymReplace=False)
    name_ORG = entityDict["name_ORG"]
    name_ORG = normalizeName(name_ORG, removeShortWords=False, doSynonymReplace=False)
    if name_PER == name_ORG:
        return None
    return (entityDict["id_ORG"],entityDict["name_ORG"])  

def createKeysFromNames(nerResultDict):
    """
    Turns each name in 'named_entities' into a key (keys) by normalization (turning lowercase and removing len 2 and less words).
    Splits multiple possibility names (separated by "|").
    """
    named_entities = nerResultDict['named_entities']
    namedEntityList = []
    for oneNameBlock in list(set(named_entities)): # For each unique "Myname|Mynamealt"
        oneEntity = NamedEntity()
        oneEntity.name = oneNameBlock
        nameAlternatives = explodeStringIntoAlternateForms(oneEntity.name)
        oneEntity.allowMatchingAsPER = checkMatchabilityPER(nameAlternatives)
        normalizedKeys = normalizeNamedEntityKeys(nameAlternatives) # Normalize and remove duplicate keys
        oneEntity.keys = normalizedKeys
        namedEntityList.append(oneEntity)
    nerResultDict["distinctNames"] = [oneEntity.name for oneEntity in namedEntityList]
    nerResultDict["normalizedNamedEntities"] = namedEntityList
    return nerResultDict

def checkMatchabilityPER(nameAlternatives):
    """
    Returns False if name cannot be a proper PER name (contains a lowercase word), True otherwise.
    """
    excludeSet = excludeSet_nonNormalized_broadcast.value
    for oneName in nameAlternatives:
        if oneName in excludeSet:
            return False
    return True

def explodeStringIntoAlternateForms(inputString):
    """
    Explodes an input string into alternate forms list based on "|" symbol and spaces.
    """
    if (not "|" in inputString):
        return [inputString]
    nameAlternatives = []
    spaceSplitParts = inputString.split(" ")
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
    return nameAlternatives

def removeDuplicatesFromNerResult(nerResultDict):
    """
    Removes duplicate named entities from "normalizedNamedEntities" in nerResultDict.
    """
    named_entities = nerResultDict["normalizedNamedEntities"]
    nerResultDict["normalizedNamedEntities"] = list(set(named_entities))
    return nerResultDict

def performKeyMatching(nerInputDict,knownEntityKeySet_broadcast_PER,knownEntityKeySet_broadcast_ORG):
    """
    Attempts to match each unknown named entity key to known entity keys.
    Input: 
        nerInputDict - dictionary with "normalizedNamedEntities" to be matched
        knownEntityKeySet_broadcast_PER - set of keys of known persons 
        knownEntityKeySet_broadcast_ORG - set of keys of known organizations
    """
    if not "normalizedNamedEntities" in nerInputDict.keys():
        return nerInputDict
    if flag_ignoreNamedEntityLabels == False and not "named_entity_labels" in nerInputDict.keys():
        return nerInputDict 
    normalizedNamedEntities = nerInputDict["normalizedNamedEntities"]
    
    # Remove all excluded names
    normalizedNamedEntities, excludedNames = filterExcludedNames(normalizedNamedEntities,excludeSet_broadcast)
    # Replace synonyms with long name forms
    normalizedNamedEntities = replaceSynonyms(normalizedNamedEntities, synonymDict_broadcast)
    
    # find matches to known persons and organizations based on normalized keys for each name
    # we don't assume type tags to be correct, therefore try matching to both PER and ORG for all names
    matchCheckedNamedEntities_PER = [checkMatchExistence(oneNamedEntity,knownEntityKeySet_broadcast_PER, filterPER=True) for oneNamedEntity in normalizedNamedEntities]
    matchCheckedNamedEntities_ORG = [checkMatchExistence(oneNamedEntity,knownEntityKeySet_broadcast_ORG, filterPER=False) for oneNamedEntity in normalizedNamedEntities]
    # retrieve matchful names (at least one candidate exists in database)
    matchfulNamedEntities_PER = [oneNamedEntity for oneNamedEntity in matchCheckedNamedEntities_PER if len(oneNamedEntity.keys) > 0]
    matchfulNamedEntities_ORG = [oneNamedEntity for oneNamedEntity in matchCheckedNamedEntities_ORG if len(oneNamedEntity.keys) > 0]
    # retrieve matchless names (no candidates exist in database)
    matchlessNamedEntities_PER = [oneNamedEntity for oneNamedEntity in matchCheckedNamedEntities_PER if len(oneNamedEntity.keys) == 0]
    matchlessNamedEntities_ORG = [oneNamedEntity for oneNamedEntity in matchCheckedNamedEntities_ORG if len(oneNamedEntity.keys) == 0]
    matchlessNames_PER = [oneNamedEntity.name for oneNamedEntity in matchlessNamedEntities_PER]
    matchlessNames_ORG = [oneNamedEntity.name for oneNamedEntity in matchlessNamedEntities_ORG]
    matchlessNames = list( set.intersection(set(matchlessNames_PER),set(matchlessNames_ORG)) ) 
    outputDict = dict()
    outputDict["pageId"] = nerInputDict["pageId"]
    outputDict["distinctNames"] = nerInputDict["distinctNames"]
    outputDict["matchfulNamedEntities_PER"] = matchfulNamedEntities_PER    
    outputDict["matchfulNamedEntities_ORG"] = matchfulNamedEntities_ORG
    outputDict["matchlessNames"] = matchlessNames
    outputDict["excludedNames"] = excludedNames
    if flag_doLabelPairOutput: 
        outputDict["nameLabelPairs"] = nerInputDict["nameLabelPairs"]
    return outputDict
        
def checkMatchExistence(oneNamedEntity, knownNamesColl_broadcast, filterPER = False):
    """
    Matches a (name,[keys]) pair to a collection of known names.
    Returns (name,[matching_keys]).
    """
    outEntity = NamedEntity()
    knownNamesColl = knownNamesColl_broadcast.value
    outEntity.name = oneNamedEntity.name
    if filterPER == True:
        if oneNamedEntity.allowMatchingAsPER == False:
            return outEntity
    outEntity.keys = filter(lambda oneKey : oneKey in knownNamesColl, oneNamedEntity.keys)
    return outEntity

def filterExcludedNames(normalizedNamedEntities,excludeSet_broadcast):
    """
    Excludes entities from normalizedNamedEntities if each key of the entitity is included in excludeSet_broadcast.value.
    Returns [includedEntities], [excludedNames].
    """
    excludeSet = excludeSet_broadcast.value
#    excludedNameList = [oneName for oneName in normalizedNamedEntities if (oneName in excludeSet)]
#    includedNameList = [oneName for oneName in normalizedNamedEntities if (oneName not in excludeSet)]

    includedEntities = []
    excludedNames = []
    
    for oneEntity in normalizedNamedEntities:
        includedKeys = []
        for oneKey in oneEntity.keys:
            if oneKey not in excludeSet:
                includedKeys.append(oneKey)
        if len(includedKeys) > 0:
            oneEntity.keys = includedKeys
            includedEntities.append(oneEntity)
        else:
            excludedNames.append(oneEntity.name)
    return includedEntities, excludedNames

def replaceSynonyms(normalizedNamedEntities, synonymDict_broadcast):
    """
    Replaces each key in normalizedNamedEntitites.keys that exists in synonymDict_broadcast.keys() with value from synonymDict_broadcast[key].
    Returns [normalizedNamedEntities] where keys with synonyms are replaced with long name forms.
    """
    synonymDict = synonymDict_broadcast.value
    replacedEntities = []
    for oneEntity in normalizedNamedEntities:
        newKeys = []
        for oneKey in oneEntity.keys:
            if oneKey in synonymDict.keys():
                newKeys.append(synonymDict[oneKey])
            else:
                newKeys.append(oneKey)
        oneEntity.keys = newKeys
        replacedEntities.append(oneEntity)
    return replacedEntities

def performDataRetrieval(onePageDict,knownEntityDict_broadcast_PER,knownEntityDict_broadcast_ORG):
    """
    For each key in onePageDict["matchfulNamedEntities_PER"] and onePageDict["matchfulNamedEntities_ORG"],
    retrieve the matching known entity data from knownEntityDict_broadcast_PER or knownEntityDict_broadcast_ORG.
    If multiple entities match the same key, attempt to find a correct PER-ORG pair using line hashes. 
    """
    matchfulNamedEntities_PER = onePageDict["matchfulNamedEntities_PER"]
    matchfulNamedEntities_ORG = onePageDict["matchfulNamedEntities_ORG"]
    
    # Retrieve line hashes and single possibility entity data
    hashSet_PER, singleMatch_key_hashesAndEntityData_PairList_PER, multipleMatches_key_listOfHashesAndEntityDataPairs_PairList_PER = retrieveDataAndHashes(matchfulNamedEntities_PER,knownEntityDict_broadcast_PER)
    hashSet_ORG, singleMatch_key_hashesAndEntityData_PairList_ORG, multipleMatches_key_listOfHashesAndEntityDataPairs_PairList_ORG = retrieveDataAndHashes(matchfulNamedEntities_ORG,knownEntityDict_broadcast_ORG)
   
    # Cross-reference multiple possibility entities to line hashes
    crossmatchable_key_listOfHashesAndEntityDataPairs_PairList_PER, nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList_PER = extractCrossmatchableEntities(multipleMatches_key_listOfHashesAndEntityDataPairs_PairList_PER,hashSet_ORG)
    crossmatchable_key_listOfHashesAndEntityDataPairs_PairList_ORG, nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList_ORG = extractCrossmatchableEntities(multipleMatches_key_listOfHashesAndEntityDataPairs_PairList_ORG,hashSet_PER)

    # Perform cross-matching between PER and ORG to narrow down results
    newMatchesFound = False
    allMatched_key_hashesAndEntityData_PairList_PER = singleMatch_key_hashesAndEntityData_PairList_PER
    allMatched_key_hashesAndEntityData_PairList_ORG = singleMatch_key_hashesAndEntityData_PairList_ORG

    # Cross-match unmatched PER to successfully matched ORG 
    crossmatched_PER, noncrossmatched_PER, newMatchesFound = performCrossmatching(crossmatchable_key_listOfHashesAndEntityDataPairs_PairList_PER,allMatched_key_hashesAndEntityData_PairList_ORG)
    #crossmatched_key_hashesAndEntityData_PairList_PER, noncrossmatched_key_listOfHashesAndEntityDataPairs_PairList_PER, newMatchesFound
    crossmatched_ORG, noncrossmatched_ORG, newMatchesFound = performCrossmatching(crossmatchable_key_listOfHashesAndEntityDataPairs_PairList_ORG,allMatched_key_hashesAndEntityData_PairList_PER)
    
    #allMatched_key_hashesAndEntityData_PairList_PER += crossmatched_PER
    #crossmatchable_key_hashesAndEntityData_PairList_PER = noncrossmatched_PER
    #allMatched_key_hashesAndEntityData_PairList_ORG += crossmatched_ORG
    #crossmatchable_key_hashesAndEntityData_PairList_ORG = noncrossmatched_ORG

    # Names with exactly one match
    singleMatched_PER = [(oneKey_hashesAndEntityDataPair[0],oneKey_hashesAndEntityDataPair[1][1]) for oneKey_hashesAndEntityDataPair in singleMatch_key_hashesAndEntityData_PairList_PER]
    singleMatched_ORG = [(oneKey_hashesAndEntityDataPair[0],oneKey_hashesAndEntityDataPair[1][1]) for oneKey_hashesAndEntityDataPair in singleMatch_key_hashesAndEntityData_PairList_ORG]
    crossmatched_PER = [(oneKey_hashesAndEntityDataPair[0],oneKey_hashesAndEntityDataPair[1][1]) for oneKey_hashesAndEntityDataPair in crossmatched_PER]
    crossmatched_ORG = [(oneKey_hashesAndEntityDataPair[0],oneKey_hashesAndEntityDataPair[1][1]) for oneKey_hashesAndEntityDataPair in crossmatched_ORG]
    
    # Names with two or more matches
    noncrossmatched_PER = [(oneKey_hashesAndEntityDataPair[0]) for oneKey_hashesAndEntityDataPair in noncrossmatched_PER]
    noncrossmatched_ORG = [(oneKey_hashesAndEntityDataPair[0]) for oneKey_hashesAndEntityDataPair in noncrossmatched_ORG]
    
    # Names with no matches
    notmatchable_PER = [(oneKey_hashesAndEntityDataPair[0]) for oneKey_hashesAndEntityDataPair in nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList_PER]
    notmatchable_ORG = [(oneKey_hashesAndEntityDataPair[0]) for oneKey_hashesAndEntityDataPair in nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList_ORG]
    notmatchable_PER = filter(lambda oneName : oneName not in noncrossmatched_PER, notmatchable_PER) # Otherwise includes noncrossmatched
    notmatchable_ORG = filter(lambda oneName : oneName not in noncrossmatched_ORG, notmatchable_ORG)
    crossmatched_nameOnly_PER = [onePair[0] for onePair in crossmatched_PER]
    crossmatched_nameOnly_ORG = [onePair[0] for onePair in crossmatched_ORG]
    notmatchable_PER = filter(lambda oneName : oneName not in crossmatched_nameOnly_PER, notmatchable_PER) # Otherwise includes crossmatched
    notmatchable_ORG = filter(lambda oneName : oneName not in crossmatched_nameOnly_ORG, notmatchable_ORG)
    
    # Add results to result dictionary 
    onePageDict["singleMatchedEntities_PER"] = singleMatched_PER
    onePageDict["crossmatchedEntities_PER"] = crossmatched_PER
    onePageDict["nonCrossmatchedNames_PER"] = noncrossmatched_PER
    onePageDict["notMatchableNames_PER"] = notmatchable_PER

    onePageDict["singleMatchedEntities_ORG"] = singleMatched_ORG
    onePageDict["crossmatchedEntities_ORG"] = crossmatched_ORG
    onePageDict["nonCrossmatchedNames_ORG"] = noncrossmatched_ORG
    onePageDict["notMatchableNames_ORG"] = notmatchable_ORG

    del onePageDict["matchfulNamedEntities_PER"]
    del onePageDict["matchfulNamedEntities_ORG"]
    
    return onePageDict
            
def retrieveDataAndHashes(matchfulNamedEntities,knownEntityDict_broadcast):
    """
    For each name key in matchedNamedEntitityKeys, retrieve the corresponding entity data from knownEntityDict_broadcast.
    Returns set of all hashes, entity data of single-matches, hash-and-entity-data of multi-matches.
    """            
    knownEntityDict = knownEntityDict_broadcast.value
    allHashes = []
    singleMatch_key_hashesAndEntityData_PairList = []
    multipleMatches_key_listOfHashesAndEntityDataPairs_PairList = []
    
    for oneMatchfulNamedEntity in matchfulNamedEntities:
        for oneMatchfulKey in oneMatchfulNamedEntity.keys:
            knownEntities_hashes_entityData_PairList = list(knownEntityDict[oneMatchfulKey]) ##  ([hashes],(name,id)) pairs list
            if len(knownEntities_hashes_entityData_PairList) == 1: ## A single known entity matches the key
                oneKnownEntity_hashes_entityData_Pair = knownEntities_hashes_entityData_PairList.pop()
                hashes = list(oneKnownEntity_hashes_entityData_Pair[0])
                allHashes = allHashes + list(hashes)
                key_hashesAndEntityData_Pair = (oneMatchfulKey, oneKnownEntity_hashes_entityData_Pair)
                singleMatch_key_hashesAndEntityData_PairList.append(key_hashesAndEntityData_Pair)
            elif len(knownEntities_hashes_entityData_PairList) > 1: ## Multiple known entities match the key
                oneKnownEntity_hashes_entityData_PairList = []
                for oneKnownEntity_hashes_entityData_Pair in knownEntities_hashes_entityData_PairList:  
                    hashes = list(oneKnownEntity_hashes_entityData_Pair[0])
                    allHashes = allHashes + list(hashes)
                    keyAndHashesAndEntityDataPair = (oneMatchfulKey, oneKnownEntity_hashes_entityData_Pair) 
                    oneKnownEntity_hashes_entityData_PairList.append(oneKnownEntity_hashes_entityData_Pair) 
                multipleMatches_key_listOfHashesAndEntityDataPairs_PairList.append( (oneMatchfulKey, oneKnownEntity_hashes_entityData_PairList) ) ## (key, [([hashes], (id, name)]))
    hashesSet = set(allHashes)
    return hashesSet, singleMatch_key_hashesAndEntityData_PairList, multipleMatches_key_listOfHashesAndEntityDataPairs_PairList


def extractCrossmatchableEntities(multipleMatches_key_listOfHashesAndEntityDataPairs_PairList,otherTypeHashSet):
    """
    Checks if multiple possibilities for matching entities can be narrowed down by PER-ORG data.
    If a common hash exists between an ORG entity and any entity from PER (or vice versa), a cross-matching possibility exists.
    If no matching hashes are found, then it is not possible to perform cross-matching.
    Returns list of cross-matchable and list of non-cross-matchable in format [ (key, ([hashes],(id, name)) ]
    """
    crossmatchable_key_listOfHashesAndEntityDataPairs_PairList = []
    nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList = []
    for oneKey_listOfhashesAndEntityDataPairs in multipleMatches_key_listOfHashesAndEntityDataPairs_PairList: ## list of (key, [([hashes], (id, name)]) )
        oneKey = oneKey_listOfhashesAndEntityDataPairs[0] # key
        one_ListOfHashesAndEntityDataPairs = oneKey_listOfhashesAndEntityDataPairs[1] # [([hashes], (id, name)]
        matchFound = False
        oneKey_crossMatchable_hashesAndEntityDataPairs_List = []
        oneKey_nonCrossMatchable_hashesAndEntityDataPairs_List = []
        for oneEntity_HashesAndEntityDataPair in one_ListOfHashesAndEntityDataPairs: ## Cross-reference entity candidate hashes to other type hashes
            hashes = list(oneEntity_HashesAndEntityDataPair[0])
            if len(set.intersection(set(hashes),otherTypeHashSet)) > 0: ## At least one cross-matching candidate exists
                oneKey_crossMatchable_hashesAndEntityDataPairs_List.append(oneEntity_HashesAndEntityDataPair)
            else: ## no cross-matching candidates exist
                oneKey_nonCrossMatchable_hashesAndEntityDataPairs_List.append(oneEntity_HashesAndEntityDataPair)
        crossmatchable_key_listOfHashesAndEntityDataPairs_PairList.append( (oneKey,oneKey_crossMatchable_hashesAndEntityDataPairs_List) )
        nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList.append( (oneKey,oneKey_nonCrossMatchable_hashesAndEntityDataPairs_List) )
    return crossmatchable_key_listOfHashesAndEntityDataPairs_PairList, nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList

def performCrossmatching(crossmatchable_key_listOfHashesAndEntityDataPairs_PairList,matchedOtherType_key_hashesAndEntityData_PairList):
    """
    Performs cross-matching between unmatched entities of one type and matched entities of other type using line hash matching.
    """
    newMatchesFound = False
    crossmatched_key_hashesAndEntityData_PairList = []
    noncrossmatched_key_listOfHashesAndEntityDataPairs_PairList = []
    for oneMatchable_key_listOfHashesAndEntityDataPairs_Pair in crossmatchable_key_listOfHashesAndEntityDataPairs_PairList: ## for each normalized key
        oneKey = oneMatchable_key_listOfHashesAndEntityDataPairs_Pair[0]
        oneKey_listOfHashesAndEntityDataPairs = oneMatchable_key_listOfHashesAndEntityDataPairs_Pair[1]
        oneKey_nonCrossmatched_hashesAndEntityDataPairs_List = []
        matchesForOneKey = False
        for oneEntity_hashesAndEntityData_Pair in oneKey_listOfHashesAndEntityDataPairs: ## For each hashes-entitydata pair
            oneEntityHashes = oneEntity_hashesAndEntityData_Pair[0]
            oneEntityData = oneEntity_hashesAndEntityData_Pair[1]
            oneEntity_crossMatched = []
            tooManyMatches = False
            for oneMatchedOtherType_key_entityData_Pair in matchedOtherType_key_hashesAndEntityData_PairList: ## For each successfully matched entity
                oneMatchedKey = oneMatchedOtherType_key_entityData_Pair[0]
                oneMatchedHashesAndEntityDataPair = oneMatchedOtherType_key_entityData_Pair[1]
                oneMatchedHashes = oneMatchedHashesAndEntityDataPair[0]
                oneMatchedEntityData = oneMatchedHashesAndEntityDataPair[1]
                if len(set.intersection(set(oneEntityHashes),set(oneMatchedHashes))) > 0:# If hashes match, it is one of the possible matches.
                    oneEntity_crossMatched.append((oneKey, oneMatchedKey))
                    if len(oneEntity_crossMatched) > 1: # If more than one other type entity match, then we are unable to narrow down using this method.
                        break
            if len(oneEntity_crossMatched) == 1: ## One crossmatch, therefore successful matching
                crossmatched_key_hashesAndEntityData_PairList.append((oneKey,oneEntity_hashesAndEntityData_Pair))
                matchesForOneKey = True
                newMatchesFound = True
            else: ## More than one match or no matches, therefore unable to crossmatch
                oneKey_nonCrossmatched_hashesAndEntityDataPairs_List.append(oneEntity_hashesAndEntityData_Pair)
        if not matchesForOneKey:    
            noncrossmatched_key_listOfHashesAndEntityDataPairs_PairList.append((oneKey,oneKey_nonCrossmatched_hashesAndEntityDataPairs_List))  
    return crossmatched_key_hashesAndEntityData_PairList, noncrossmatched_key_listOfHashesAndEntityDataPairs_PairList, newMatchesFound

def explodeOutputForEachName(matchedInputDict):
    """
    For each detected name, explode into a list of strings in format date::host::path::name. 
    """
    pageId = matchedInputDict["pageId"]
    distinctNames = matchedInputDict["distinctNames"]
    return [pageId + "::" + oneName for oneName in distinctNames]
    
def explodeOutputForEachInCategory(inputDict,categoryName):
    """
    For each element in inputDict[categoryName], explode into a list of strings in format date::host::path::element. 
    """
    try:
        hostname = matchedInputDict["hostname"]
        path = matchedInputDict["path"]
        date = matchedInputDict["date"]
        elementList = inputDict[categoryName]
        return [hostname + "::" + path + "::" + date + "::" + str(oneElement) for oneElement in elementList]
    except:
        return
    
def explodeNamesAndKeys(inputDict):
    """
    Returns (name,[keys]) pairs list from "normalizedNamedEntities" in input dict. 
    """

    entities = inputDict["normalizedNamedEntities"]
    nameKeyPairList = []
    for oneEntity in entities:
        name = oneEntity.name
        keys = oneEntity.keys
        nameKeyPairList.append((name,keys))
    return nameKeyPairList    
        
def textifyKeys_PER(oneKeyDataPair):
    """
    Returns ("firstname lastname", ([ [matching_line_hashes],(per_id,per_firstname,per_lastname) ) ]) format as plaintext.
    """
    #("firstname lastname", ([ [matching_line_hashes],(per_id,per_firstname,per_lastname) ) ])
    outString = ""
    key = oneKeyDataPair[0]
    hashesDataPair = list(oneKeyDataPair[1])
    hashes = hashesDataPair[0][0]
    data_PER = hashesDataPair[0][1]
    id_PER = data_PER[0]
    firstName_PER = data_PER[1]
    lastName_PER = data_PER[2]
    outString = u",".join([unicode(key),id_PER,firstName_PER,lastName_PER,unicode(len(hashes))])
    #outString = str(key) + "," + id_PER +","+firstName_PER+","+lastName_PER+","+str(len(hashes))
    return outString

def textifyKeys_ORG(oneKeyDataPair):
    """
    Returns ("orgname", ([ [matching_line_hashes],(org_id,org_name) ) ]) format as plaintext.
    """
    #("orgname", ([ [matching_line_hashes],(org_id,org_name) ) ])
    outString = ""
    key = oneKeyDataPair[0]
    hashesDataPair = list(oneKeyDataPair[1])
    hashes = hashesDataPair[0][0]
    data_ORG = hashesDataPair[0][1]
    id_ORG = data_ORG[0]
    name_ORG = data_ORG[1]
    outString = u",".join([unicode(key),id_ORG,name_ORG,unicode(len(hashes))])
    #outString = str(key) + "," + id_ORG +","+name_ORG+","+str(len(hashes))
    return outString

def processKnownEntities():
    """
    Reads known entity data from CSV file and creates a dictionary with normalized names as keys and matching data as values.
    """
    print("Processing known entity data csv file...")
    knownEntityProcessingStartTime = time.time()
    # load file with known entity data (person + organization data rows)
    knownEntitiesFile = spark.textFile(knownEntityPath,use_unicode=True)

    # parse known entity file lines into tuples
    # filter out invalid lines
    # result pair : (lineHash, lineDict)
    lineHash_lineDict_PairRDD = knownEntitiesFile.map(lambda line: parseEntities(line)).filter(lambda entry: entry is not None)
    lineHash_lineDict_PairRDD.cache()
    # Extract person info from known entity line dictionaries
    # Group line hashes by PER data (each person has a list of hashes of matching lines)
    # result pair : ((per_id, per_firstname, per_lastname), [line_hashes])    
    personData_LineHashes_PairRDD = lineHash_lineDict_PairRDD.map(lambda entity: (extractPerFromEntityLineDict(entity[1]), entity[0]) )
    personData_LineHashes_PairRDD = personData_LineHashes_PairRDD.filter(lambda entity : len(entity[0][1]) > 0 and len(entity[0][2]) > 0) # Exclude names with no firstname or lastname
    personData_LineHashes_PairRDD = personData_LineHashes_PairRDD.groupByKey()
    # NOTE: Known PER names are not lemmatized
    # Normalize name for PER matching (lowercase with len <3 words removed)
    # result pair : ("firstname lastname", ([ [matching_line_hashes],(per_id,per_firstname,per_lastname) ) ])
    matchableName_HashAndData_PairRDD_PER = personData_LineHashes_PairRDD.map(lambda entity : (normalizeName(u" ".join([ entity[0][1],entity[0][2] ]), removeShortWords=False,doSynonymReplace=False), (entity[1], entity[0]) ) )
 
    # Extract organization info from known entity lines
    # Group line hashes by ORG data (each organization has a list of hashes of matching lines)
    # result pair : ((org_id, org_name), [line_hashes]) 
    orgData_LineHashes_PairRDD = lineHash_lineDict_PairRDD.map(lambda entity: (extractOrgFromEntityLineDict(entity[1]), entity[0]) ).filter(lambda onePair : onePair[0] is not None).groupByKey()
    if flag_lemmatizeKnownNames: # If enabled, create additional alternative keys for known entities
#             knownEntityRDD = orgData_LineHashes_PairRDD.map(lambda entity: (entity[0], lemmatizeName(entity[0][1])) )
#             knownEntityRDD.saveAsTextFile(outputPath + "/known_ORG")
        orgData_LineHashes_PairRDD = orgData_LineHashes_PairRDD.flatMap(lambda pair : lemmatizeAndPairWithData(pair, type="org"))
    # Normalize name for ORG matching (lowercase with len <3 words removed)
    # result pair : ("orgname", ([ [matching_line_hashes],(org_id,org_name) ) ])
    matchableName_HashAndData_PairRDD_ORG = orgData_LineHashes_PairRDD.map(lambda entity : (normalizeName( unicode(entity[0][1]),removeShortWords=False,doSynonymReplace=True ), ( entity[1], entity[0] ) ))
    if flag_doKnownEntityOutput: # If enabled, output entity and keys to file
        matchableName_HashAndData_PairRDD_ORG.saveAsTextFile(outputPath + "/known_ORG")

    if flag_doAllKnownEntityKeysOutput: ## if true, output all matchable keys to files
        matchableName_HashAndData_PairRDD_PER.groupByKey().map(lambda oneKeyDataPair : textifyKeys_PER(oneKeyDataPair) ).saveAsTextFile(outputPath+"/data_PER")
        matchableName_HashAndData_PairRDD_ORG.groupByKey().map(lambda oneKeyDataPair : textifyKeys_ORG(oneKeyDataPair) ).saveAsTextFile(outputPath+"/data_ORG")
    knownEntityProcessingEndTime = time.time()
    print("Known entity file processed in: " + str(knownEntityProcessingEndTime-knownEntityProcessingStartTime))
    return matchableName_HashAndData_PairRDD_PER, matchableName_HashAndData_PairRDD_ORG

if __name__ == "__main__":
    """
    Executes NER and known entity matching.
    """
    
    flag_doAllKnownEntityKeysOutput = False ## if True, output the lists of all known entity keys to data_PER and data_ORG directories 
    flag_ignoreNamedEntityLabels = True ## if True, ignore PER and ORG tags found from NLTK
    flag_doMatchingOutput = False ## If True, output matched/unmatched keys to "/matching" directory
    flag_doLabelPairOutput = True ## If True, output "name_label_pairs" into main output file
    flag_doKnownEntityOutput = False ## If True, output known entity name and key values to "/known_<type>" directory
    flag_explodeAlternatives = True ## If True, explode "|"-separated alternative name forms to separate names
    flag_outputExplodedAlternatives = False ## If True, output NLTK-detected names in exploded form. Otherwise, use "|"-separated form. 
    flag_explodeOutputForNames = False ## If True, output into "explodedNames" directory each name on a separate line with page URI
    flag_explodeOutputForSuccessfulMatches = False ## If True, output into "explodedResults_<categoryName>" each successful match on a separate line with page URI
    flag_lemmatizeKnownNames = False # If True, apply lemmatization to known names before normalizing
    flag_processKnownEntities = False # If True, read from CSV file and process known entity data. Otherwise assume it has been done and read from knownEntityDir.
    flag_removeSubnames = True # If True, ignore each name that is a substring of another name in the same URI location. 
    
    # Word-replacament dictionary to use in name normalization
    orgtypeReplaceDict = {u"aktsiaselts":u"as", u"korteriühistu":u"kü", u"osaühing":u"oü", u"mittetulundusühing":u"mtü", u"sihtasutus":u"sa"}
    
    inputEncoding = "utf-8"
    lemmatizedNamePartMultiplier = 0.8 # Multiplier for name part threshold in known entity name lemmatization
    
    # List of exclusion file tags: exclude tagged names from unknown name matching 
    excludeTagList = [u"event",u"loc",u"product",u"nan",u"famous_per",u"famous_org"]
    synonymTagList = [u"synonym"]
    
    helpString = """nerMatcher.py <nerPath> <knownEntitiesPath> <outputPath> [args]
                \n -m, --manifest : Text file containing spark_estnltk NER result file paths to be processed.
                \n -k, --known : Directory with known entities CSV or pickle files with processed known entity data (created by --processKnown)
                \n -o, --out : Output directory. Has to be non-existing.
                \n -x, --exclude : Directory containing CSV file of names to exclude from unknown entity name matching.
                \n --processKnown : Read and process known entity data from CSV file(s) in knownEntitiesPath, then save results to knownEntitiesPath/KEDict_PER and KEDict_ORG. If omitted, assume known entities are already processed and read them from knownEntitiesPath/KEDict_PER and KEDict_ORG.  
                \n --lemmatizeKnown : Performs ESTNLTK lemmatization on known entity names before matching. Implicitly performs --processKnown.
                \n --explodeOutputForNames : For all unknown entity names, output URI-name pairs to explodedNames directory in output directory
                \n --explodeOutputForMatches : For all successful matches, output URI-match pairs to explodedSuccesses_<type> directories 
                """
    
    if len(sys.argv) < 4:
        print len(sys.argv)
        print helpString
        sys.exit()

    startTime = time.time()
    
    manifestPath = ""
    knownEntityPath = ""
    outputPath = ""
    excludePath = ""
    try:
        opts, args = getopt.getopt(sys.argv[1:], "m:k:o:x:h:", ["manifest=","known=","out=","exclude=","outputExplodedAlternatives","explodeOutputForNames","explodeOutputForMatches","lemmatizeKnown","processKnown","help"])
    except getopt.GetoptError:
        print 'ARGUMENT ERROR'
        print helpString
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("--outputExplodedAlternatives"):
            flag_outputExplodedAlternatives = True
        if opt in ("--explodeOutputForNames"):
            flag_explodeOutputForNames = True
        if opt in ("--explodeOutputForMatches"):
            flag_explodeOutputForSuccessfulMatches = True
        if opt in ("--lemmatizeKnown"):
            flag_lemmatizeKnownNames = True
            flag_processKnownEntities = True
        if opt in ("--processKnown"):
            flag_processKnownEntities = True
        if opt in ("-m","--manifest"):
            manifestPath = arg
        if opt in ("-k","--known"):
            knownEntityPath = arg
        if opt in ("-o","--out"):
            outputPath = arg
        if opt in ("-x","--exclude"):
            excludePath = arg
        if opt in ("-h","--help"):
            print helpString
            sys.exit(2)    
        if manifestPath == "":        
            manifestPath = arg
        if knownEntityPath == "":
            knownEntityPath = arg
        if outputPath == "":
            outputPath = arg
    
    # Ensure that the output directory does not exist
    print("MANIFEST: " + manifestPath)    
    print("KNOWN: " + knownEntityPath)
    print("OUT: " + outputPath)
    print("EXCLUDE: " + excludePath)
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
    # Exclusions and synonyms: Process exclusion/synonym files, create exclusion and synonym broadcasts
    ################
    print("Excluding from exclusion file...")
    fromFileExcludeSet = set()
    nonNormalizedExcludeSet = set()
    synonymDict = {}
    if (excludePath != ""):
        excludeFile = spark.textFile(excludePath,use_unicode=True)
        unfilteredExcludeRDD = excludeFile.map(lambda oneRow : processExcludeRow(oneRow)).filter(lambda oneDict: oneDict is not None)
        # RDD of names to ignore
        excludeRDD = unfilteredExcludeRDD.filter(lambda oneDict : oneDict["tag1"] in excludeTagList).map(lambda oneDict : oneDict["name"])      
        normalizedExcludeRDD = excludeRDD.map(lambda oneName : normalizeName(oneName, removeShortWords=False,doSynonymReplace=False))

        # (name,synonym) pair RDD
        synonymRDD = unfilteredExcludeRDD.filter(lambda oneDict : oneDict["tag1"] in synonymTagList).map(lambda oneDict : (oneDict["name"],oneDict["synonym"]) )
        normalizedSynonymRDD = synonymRDD.map(lambda onePair : ( normalizeName(onePair[0],removeShortWords=False,doSynonymReplace=False), normalizeName(onePair[1],removeShortWords=False, doSynonymReplace=False) ) )
        
        fromFileExcludeSet = set(normalizedExcludeRDD.collect())
        nonNormalizedExcludeSet = set(excludeRDD.collect())
        synonymDict = normalizedSynonymRDD.collectAsMap()
    
    # Add to general exclusion set
    
    excludeSet = set()
    excludeSet = excludeSet.union(fromFileExcludeSet)
    # CONSIDER INCLUDING: Remove matchable ORG names from exclude list (may defeat the purpose of exclusions)
    # May need a clean list of non-excludable names (add to exclusion/synonym file?)
    #excludeSet = excludeSet.difference(knownEntityKeySet_broadcast_ORG.value)
    
    # Broadcast exclusions
    excludeSet_broadcast = spark.broadcast(excludeSet)
    excludeSet_nonNormalized_broadcast = spark.broadcast(nonNormalizedExcludeSet)
    
    # Broadcast synonyms
    synonymDict_broadcast = spark.broadcast(synonymDict)
    
    print ("EXCLUDESET LEN : " + str(len(excludeSet_broadcast.value)))
    print ("SYNONYMDICT LEN : " + str(len(synonymDict_broadcast.value)))

    ################
    #  Process known entity data into matchable names
    ################
    if flag_processKnownEntities: # Read from CSV, process lines, save to Pickle file
        call(["hadoop", "fs", "-rm", "-r", knownEntityPath + "/KEDict_PER"])
        call(["hadoop", "fs", "-rm", "-r", knownEntityPath + "/KEDict_ORG"])
        matchableName_HashAndData_PairRDD_PER, matchableName_HashAndData_PairRDD_ORG = processKnownEntities()
        groupedByName_HashAndData_PairRDD_PER = matchableName_HashAndData_PairRDD_PER.groupByKey()
        matchableName_HashAndData_PairRDD_PER.unpersist()
        groupedByName_HashAndData_PairRDD_ORG = matchableName_HashAndData_PairRDD_ORG.groupByKey()
        matchableName_HashAndData_PairRDD_ORG.unpersist()
        groupedByName_HashAndData_PairRDD_PER.saveAsPickleFile(knownEntityPath + "/KEDict_PER")
        groupedByName_HashAndData_PairRDD_ORG.saveAsPickleFile(knownEntityPath + "/KEDict_ORG")
    else: # Read processed data from Pickle file
        groupedByName_HashAndData_PairRDD_PER = spark.pickleFile(knownEntityPath + "/KEDict_PER")
        groupedByName_HashAndData_PairRDD_ORG = spark.pickleFile(knownEntityPath + "/KEDict_ORG")
    
    print ("Broadcasting known entities...")
    knownEntityKeySet_broadcast_PER = spark.broadcast(set( groupedByName_HashAndData_PairRDD_PER.keys().collect()))
    knownEntityDict_broadcast_PER = spark.broadcast(groupedByName_HashAndData_PairRDD_PER.collectAsMap())
#     groupedByName_HashAndData_PairRDD_PER.unpersist()
    
    knownEntityKeySet_broadcast_ORG = spark.broadcast(set(groupedByName_HashAndData_PairRDD_ORG.keys().collect()))
    knownEntityDict_broadcast_ORG = spark.broadcast(groupedByName_HashAndData_PairRDD_ORG.collectAsMap())
#     groupedByName_HashAndData_PairRDD_ORG.unpersist()
    print ("Broadcasting done.")
    
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
    # normalize names to keys
    # split into multiple keys if needed ("|" separator in name)
    normalizedKeysNerResultsDictRDD = nerResultDictRDD.map(lambda nerResult : createKeysFromNames(nerResult))
    nerResultsRDD = normalizedKeysNerResultsDictRDD
    # remove duplicates
    #nerResultsRDD = normalizedKeysNerResultsDictRDD.map(lambda nerResult : removeDuplicatesFromNerResult(nerResult))

    ################
    # Exclusions : Exclude lone first names
    ################
    print("Excluding first names...")
    names = knownEntityKeySet_broadcast_PER.value
    firstnames = [oneName.split(" ")[0] for oneName in names if oneName != None]
    firstnameSet = set(firstnames)
    excludeSet = excludeSet.union(firstnameSet)
    # Broadcast exclusions
    excludeSet_broadcast = spark.broadcast(excludeSet)
    

    ################
    # Matching : match each name in each NER result dict to known entities
    ################
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
    #entityDataRDD = matchedNerResultsRDD.map(lambda oneResult : performDataRetrievalFromDistributedDict(oneResult))
    
    # Save result
    entityDataRDD.saveAsTextFile(outputPath + "/entityData")

    ###############
    # (optional) Exploded output of names:
    #  if enabled, output each name of each page on separate line with URI
    ###############
    if flag_explodeOutputForNames:
        explodedNamesRDD = matchedNerResultsRDD.flatMap(lambda oneResult : explodeOutputForEachName(oneResult))
        explodedNamesRDD.saveAsTextFile(outputPath + "/explodedNames")
    
    ###############
    # (optional) Exploded output of results:
    #  if enabled, output each successful matching on separate line with URI
    ###############
    if flag_explodeOutputForSuccessfulMatches:
        for categoryName in ["singleMatchedEntities_PER" ,"singleMatchedEntities_ORG","crossmatchedEntities_PER","crossmatchedEntities_ORG" ]:
            explodedSuccessesRDD = matchedNerResultsRDD.flatMap(lambda oneResult : explodeOutputForEachInCategory(oneResult,categoryName))
            explodedSuccessesRDD.saveAsTextFile(outputPath + "/explodedSuccesses_"+categoryName)
        
    matchedNerResultsRDD.unpersist()
    print("Finished unknown entity files.")
    oneFileEndTime = time.time()
    print("Time taken: " + str(oneFileEndTime - oneFileStartTime))
    entityDataRDD.unpersist()
    spark.stop()
    endTime = time.time()
    print "Time to finish:" + str(endTime - startTime)
