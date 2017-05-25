#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
NER result to database matching
input:
  NER values
  file with person and organization data
"""
from string import strip
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
from collections import namedtuple
from pyspark.storagelevel import StorageLevel
from pyspark import SparkContext, SparkConf
#from pyspark.shuffle import InMemoryMerger
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
import collections
import itertools
import gc
import nltk
import nerMatcherClasses
"""
Named tuple for one line (PER and ORG pair) from known entity file data file.
"""
DataLine = collections.namedtuple("DataLine", ["tag", "id_PER", "firstName_PER", "lastName_PER", "birthDate_PER", "id_ORG", "name_ORG", "startDate", "endDate", "relation"])

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
    Returns DataLine named tuple
    with names (tag,id_PER,firstName_PER,lastName_PER,birthDate_PER,id_ORG,name_ORG,startDate,endDate,relation).
    """
    """
    Expected input line formats:
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
        # tag,id_PER,firstName_PER,lastName_PER,birthDate_PER,id_ORG,name_ORG,startDate,endDate,relation
        tag = values[0]
    elif len(values) == 9:
        # id_PER,firstName_PER,lastName_PER,birthDate_PER,id_ORG,name_ORG,startDate,endDate,relation
        tag = None
    else:
        # discard other lines
        return None
    id_PER = values[1]
    firstName_PER = values[2]
    lastName_PER = values[3]
    birthDate_PER = cleanDate(values[4])
    id_ORG = values[5]
    name_ORG = values[6]
    startDate = cleanDate(values[7])
    endDate = cleanDate(values[8])
    relation = values[9]
    return DataLine(tag,id_PER,firstName_PER,lastName_PER,birthDate_PER,id_ORG,name_ORG,startDate,endDate,relation)
    
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
    Returns [normalized_keys].
    """
    keys = namedEntityKeys
    normalizedKeys = [normalizeName(oneKey, removeShortWords=False) for oneKey in keys]
    normalizedKeys = filter(lambda oneKey: oneKey is not None, normalizedKeys)
    normalizedKeys = list(set(normalizedKeys))
    return normalizedKeys

def lemmatizeAndPairWithData(entity,type="org"):
    """
    Lemmatizes name of entity in form ((id,name),data). Specify type as "per" or "org" for proper handling.
    Returns [((id,altname),data)] with alternative names.
    NOTE: PER handling is not implemented
    """
    if (type == "org"):
        name = entity[0][1]
        lemmatizedNames = lemmatizeName(name)
        # Also preserve original
        lemmatizedNames.append(name)
        lemmatizedNames = [normalizeName(name, removeShortWords=False) for name in lemmatizedNames]
        # remove duplicates after normalization
        lemmatizedNames = list(set(lemmatizedNames))
        nameEntityPairs = [((entity[0][0],oneName),entity[1]) for oneName in lemmatizedNames]
    return nameEntityPairs    
    
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

def extractPerPairFromKnownEntityDataLine(dataLine):
    """
    Returns (person_data,other_data) pair from known entity DataLine named tuple.
    """
    firstTuple = (dataLine.id_PER,dataLine.firstName_PER,dataLine.lastName_PER)
    secondTuple = (dataLine.id_ORG,dataLine.name_ORG,dataLine.startDate,dataLine.endDate,dataLine.relation) 
    return (firstTuple, secondTuple)

def extractOrgPairFromKnownEntityDataLine(dataLine):
    """
    Returns (org_data,other_data) pair from known entity DataLine named tuple.
    If PER name exactly matches ORG name, return None.
    """
    name_PER = dataLine.firstName_PER + " " + dataLine.lastName_PER
    name_PER = normalizeName(name_PER, removeShortWords=False, doSynonymReplace=False)
    name_ORG = dataLine.name_ORG
    name_ORG = normalizeName(name_ORG, removeShortWords=False, doSynonymReplace=False)
    if name_PER == name_ORG:
        return None
    firstTuple = (dataLine.id_ORG,dataLine.name_ORG)
    secondTuple = (dataLine.id_PER,dataLine.firstName_PER,dataLine.lastName_PER,dataLine.startDate,dataLine.endDate,dataLine.relation) 
    return (firstTuple, secondTuple)

def createNamedEntityObjectsFromNames(nerResultDict):
    """
    For each string in inputdict['named_entities'], create NamedEntity object with:
        'name' - string from dict[named_entitites']
        'normalizedForms' - normalized forms of name (turning lowercase and removing len 2 and less words,
            names with "|" are split into multiple keys)
        'ngramKeys' - improved keys created from order-preserving n-gram combinations (of normalized keys)
    Returns: inputdict with added 'distinctNames' (set of dict['named_entities']) 
        and added 'normalizedNamedEntities' (list of NamedEntity elements described above)
    """
    named_entities = nerResultDict['named_entities']
    sourceId = nerResultDict['pageId']
    namedEntityList = []
    for oneNameBlock in list(set(named_entities)): # For each unique "Myname|Mynamealt"
        oneEntity = nerMatcherClasses.NamedEntity()
        oneEntity.sourceId = sourceId
        oneEntity.name = oneNameBlock
        nameAlternatives = explodeStringIntoAlternateForms(oneNameBlock)
        oneEntity.allowMatchingAsPER = checkMatchabilityPER(nameAlternatives)
        normalizedKeys = normalizeNamedEntityKeys(nameAlternatives) # Normalize and remove duplicate keys
        normalizedKeys = replaceSynonyms(normalizedKeys, synonymDict_broadcast) # Replace synonyms with long name forms    
        oneEntity.normalizedForms = normalizedKeys
        ngramKeys = []
        for oneKey in normalizedKeys:
            newNgramKeys = createNgramKeys(oneKey, int(len(oneKey)*ngramLengthThresholdMult))
            ngramKeys += newNgramKeys
        sortedNgramKeys = list(set(ngramKeys))
        sortedNgramKeys.sort(key=len)
        oneEntity.ngramKeys = sortedNgramKeys
        namedEntityList.append(oneEntity)
        
    nerResultDict["distinctNames"] = [oneEntity.name for oneEntity in namedEntityList]
    nerResultDict["normalizedNamedEntities"] = namedEntityList
    return nerResultDict

def createNgrams(name, n):
    """
    Returns the list of n-length substrings of input name. 
    The substrings are ordered started from the beginning of the name.
    """
    #NOTE: NLTK ngrams may be faster than zip
    splitngrams = zip(*[name[i:] for i in range(n)])
    ngrams = []
    for onengram in splitngrams:
        combinedngram = u""
        for symbol in onengram:
            combinedngram += unicode(symbol)
        ngrams.append(combinedngram)
    return ngrams

def createNgramKeys(name, n):
    """
    Creates all n-grams with token length n and above from input name.
    Returns list of all created n-grams.
    """
    ngramKeys = []
    for i in range(n, len(name)+1):
        ngramKeys += createNgrams(name, i)
    # Always preserve original name, even if short
    if name not in ngramKeys:
        ngramKeys.append(name)
    return ngramKeys

def createNgramKeyNamePairs(name,n):
    """
    Generates all n-grams with length n or more from input name. 
    Returns list of (ngramkey,[name]) pairs.
    Calls creteNgramKeys(name,n) for generation.
    """
    name = strip(name)
    ngramKeys = createNgramKeys(name, n)
    returnList = []
    for ngramKey in ngramKeys:
        returnList.append( (ngramKey, [name]) )
    return returnList

def createNamedEntityAndDictPairs(inputDict):
    """
    Returns (NamedEntity, inputDict) pairs.
    inputDict must contain 'normalizedNamedEntities'
    """
    returnList = []
    namedEntities = inputDict['normalizedNamedEntities']
    for oneNamedEntity in namedEntities:
        returnList.append( (oneNamedEntity,inputDict) )
    return returnList

def createKeyAndNamedEntityPairs(namedEntity):
    """
    Returns (key, namedEntity) pairs.
    Each key is taken from namedEntity['ngramKeys'].
    """
    returnList = []
    keys = namedEntity.ngramKeys
    for oneKey in keys:
        returnList.append( (oneKey,namedEntity) )
    return returnList

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


def performKeyMatching(nerInputDict,ngramkey_normalizedPerNames_broadcast,ngramKey_normalizedOrgNames_broadcast):
    """
    Attempts to match each unknown named entity to a known entity.
    Uses n-grams for matching.
    Input: 
        nerInputDict - dictionary with "normalizedNamedEntities" containing NamedEntity dicts with 'name','normalizedNames','ngramKeys'
        ngramkey_normalizedPerNames_broadcast - broadcast of ngram-normalizedname pairs for PERs
        ngramKey_normalizedOrgNames_broadcast - broadcast of ngram-normalizedname pairs for ORGs
    """
    if not "normalizedNamedEntities" in nerInputDict.keys():
        return nerInputDict
    if flag_ignoreNamedEntityLabels == False and not "named_entity_labels" in nerInputDict.keys():
        return nerInputDict 
    normalizedNamedEntities = nerInputDict["normalizedNamedEntities"]
    
    # Remove all excluded names
    normalizedNamedEntities, excludedNames = filterExcludedNames(normalizedNamedEntities,excludeSet_broadcast)
    
    # find matches to known persons and organizations based on normalized keys for each name
    # we don't assume type tags to be correct, therefore try matching to both PER and ORG for all names
    matchCheckedNamedEntities_PER = [matchNamedEntityToKnownEntityKeys(oneNamedEntity,ngramkey_normalizedPerNames_broadcast, filterPER=True) for oneNamedEntity in normalizedNamedEntities]
    matchCheckedNamedEntities_ORG = [matchNamedEntityToKnownEntityKeys(oneNamedEntity,ngramKey_normalizedOrgNames_broadcast, filterPER=False) for oneNamedEntity in normalizedNamedEntities]
    
    #TODO: add key length thresholding to ignore keys that are too short
    
    # retrieve matchful names (at least one candidate exists in database)
    matchfulNamedEntities_PER = [oneNamedEntity for oneNamedEntity in matchCheckedNamedEntities_PER if len(oneNamedEntity.matchingKnownEntityKeys) > 0]
    matchfulNamedEntities_ORG = [oneNamedEntity for oneNamedEntity in matchCheckedNamedEntities_ORG if len(oneNamedEntity.matchingKnownEntityKeys) > 0]
    # retrieve matchless names (no candidates exist in database)
    matchlessNamedEntities_PER = [oneNamedEntity for oneNamedEntity in matchCheckedNamedEntities_PER if len(oneNamedEntity.matchingKnownEntityKeys) == 0]
    matchlessNamedEntities_ORG = [oneNamedEntity for oneNamedEntity in matchCheckedNamedEntities_ORG if len(oneNamedEntity.matchingKnownEntityKeys) == 0]
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

        
def matchNamedEntityToKnownEntityKeys(oneNamedEntity, ngramKey_normalizedNames_broadcast, filterPER = False):
    """
    Matches oneNamedEntity to zero or more known entities based on ngramKeys.
    If filterPER is True, perform no matching of NamedEntities where allowMatchingAsPer is False.
    Returns oneNamedEntity with added NamedEntity.matching_keys.
    """
    if filterPER == True:
        if oneNamedEntity.allowMatchingAsPER == False:
            return oneNamedEntity
        
    ngramKey_normalizedNames = ngramKey_normalizedNames_broadcast.value
    longestMatch = -1 # length of longest ngramKey with known matches
    matchingNormalizedNames = []
    for oneKey in oneNamedEntity.ngramKeys:
        if oneKey in ngramKey_normalizedNames:
            if longestMatch == -1: # if no previous matches, use this one
                matchingNormalizedNames = ngramKey_normalizedNames[oneKey]
                longestMatch = len(oneKey)
            else: # previous match exists
                if len(oneKey) == longestMatch: # if same length key, accept additional matches
                    matchingNormalizedNames += ngramKey_normalizedNames[oneKey]
                else: # if not, break
                    break
    oneNamedEntity.matchingKnownEntityKeys = list(set(matchingNormalizedNames))
    return oneNamedEntity

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

def replaceSynonyms(inputKeys, synonymDict_broadcast):
    """
    Replaces each key in inputKeys that exists in synonymDict_broadcast.keys() with value from synonymDict_broadcast[key].
    Returns key list where keys with synonyms are replaced with long name forms (others are preserved as is).
    """
    synonymDict = synonymDict_broadcast.value
    newKeys = []
    for oneKey in inputKeys:
        if oneKey in synonymDict.keys():
            newKeys.append(synonymDict[oneKey])
        else:
            newKeys.append(oneKey)
    return newKeys

def preserveLongestKeys(namedEntity_keyValPairList):
    """
    Returns input with only the highest length key tuples preserved. 
    """
    namedEntity = namedEntity_keyValPairList[0]
    keyValPairs = namedEntity_keyValPairList[1]
    longestKeyLen = 0
    preservedKeyValPairs = []
    for keyVal in keyValPairs:
        keyLen = len(keyVal[0])
        if (keyLen >= longestKeyLen):
            longestKeyLen = keyLen
            preservedKeyValPairs.append(keyVal)
    return (namedEntity, preservedKeyValPairs)

def invertToNormalizedNameKey(onePair):
    """
    Converts (NamedEntity, [(key,[normalizedname)]) pair to normalized name as pairkey.
    Returns: list of (normalizedname, (NamedEntity,key)).
    """
    namedEntity = onePair[0]
    keyNormalizedNamePairs = onePair[1]
    returnList = []
    for keyNormalizedNamePair in keyNormalizedNamePairs:
        key = keyNormalizedNamePair[0]
        normalizedNames = keyNormalizedNamePair[1]
        for normalizedName in normalizedNames:
            newPair = (normalizedName, (namedEntity, key))
            returnList.append(newPair)
    return returnList

def formatOneKeyNamedEntityNormalizedName(entity):
    """
    Input: (key, (NamedEntity,[normalizedname]))
    Returns: (NamedEntity,(key,[normalizedname]))
       if no normalizedNames: (NamedEntity,(key,[])) 
    """
    if (entity[1][1] is None):
        return (entity[1][0], (entity[0], [] ) )
    else:
        return (entity[1][0], (entity[0],entity[1][1]) )

def collectDataBySourceId(key_NamedEntityAndNormNameList_PairRDD_xx,normalizedXxName_xxYyRelPairs_PairRDD):
    """
    Input RDD: (key, (NamedEntity,[normalizedname]))
    Input RDD: (normalizedPerName, [ ((id_PER, firstName_PER, lastName_PER), [(id_ORG,name_ORG,startDate,endDate,relation)])])
    OR ORG-type analog
    Returns RDD: (sourceId, [((normalizedname, ((NamedEntity, key), [xxYyRelPairs]) ))])
    """
    # result: (NamedEntity, (key,[normalizedname])
        
    namedEntity_KeyAndNormNameList_PairRDD_xx = key_NamedEntityAndNormNameList_PairRDD_xx.map(lambda entity : formatOneKeyNamedEntityNormalizedName(entity))
    # result: (NamedEntity, [ (key,[normalizedname] ])
    namedEntity_KeyAndNormNameLists_PairRDD_xx = namedEntity_KeyAndNormNameList_PairRDD_xx.groupByKey()
    # result: (NamedEntity, [ (key,[normalizedname] ]), where only longest key tuples are preserved
    namedEntity_LongestKeyAndNormNameLists_PairRDD_xx = namedEntity_KeyAndNormNameLists_PairRDD_xx.map(lambda entity : preserveLongestKeys(entity))
    # result: (normalizedname, (NamedEntity, key))
    normalizedName_NamedEntityAndKey_PairRDD_xx  = namedEntity_LongestKeyAndNormNameLists_PairRDD_xx.flatMap(lambda onePair : invertToNormalizedNameKey(onePair))
    # result: (normalizedname, ((NamedEntity, key), [xxYyRelPairs]) )
    normalizedName_NamedEntityAndKeyAndData_PairRDD = normalizedName_NamedEntityAndKey_PairRDD_xx.join(normalizedXxName_xxYyRelPairs_PairRDD)
    # result: (sourceId, [(normalizedname, ((NamedEntity, key), [xxYyRelPairs]) )])
    sourceId_ListOfHits_PairRDD_xx = normalizedName_NamedEntityAndKeyAndData_PairRDD.map(lambda entity : (entity[1][0][0].sourceId, entity)).groupByKey()
    return sourceId_ListOfHits_PairRDD_xx


def performDataRetrieval(onePageDict,normalizedPerName_perOrgRelPairs_broadcast,normalizedOrgName_orgPerRelPairs_broadcast):
    """
    For each key in onePageDict["matchfulNamedEntities_PER"] and onePageDict["matchfulNamedEntities_ORG"],
    retrieve the matching known entity data from normalizedPerName_perOrgRelPairs_broadcast or normalizedOrgName_orgPerRelPairs_broadcast.
    If multiple entities match the same key, attempt to find a correct PER-ORG pair using line hashes. 
    """
    matchfulNamedEntities_PER = onePageDict["matchfulNamedEntities_PER"]
    matchfulNamedEntities_ORG = onePageDict["matchfulNamedEntities_ORG"]
    
    #TODO: rework to implement ngram-related modifications
    # Retrieve PER-ORGrels data for each key in NamedEntity.matchingKnownEntityKeys
    (singleMatchNamedEntities_PER, 
     multipleMatchesNamedEntities_PER
    ) = retrieveMatchingKnownEntities(matchfulNamedEntities_PER,normalizedPerName_perOrgRelPairs_broadcast)
    singleMatchNamedEntitySet_PER = set(singleMatchNamedEntities_PER) 
    multipleMatchesNamedEntitySet_PER = set(multipleMatchesNamedEntities_PER)
    # Retrieve ORG-PERrels data
    (singleMatchNamedEntities_ORG,
      multipleMatchesNamedEntities_ORG
    ) = retrieveMatchingKnownEntities(matchfulNamedEntities_ORG,normalizedOrgName_orgPerRelPairs_broadcast)
    singleMatchNamedEntitySet_ORG = set(singleMatchNamedEntities_ORG) 
    multipleMatchesNamedEntitySet_ORG = set(multipleMatchesNamedEntities_ORG)
    
    # Cross-reference multiple match candidates to single-match othertype entities
    # If a single cross-match exists, add to single-match list
    # otherwise unable to narrow down
    # Repeat if any new single-matches were added
    doCrossmatching_PER = True
    doCrossmatching_ORG = True
    crossMatchedNamedEntitySet_PER = []
    crossMatchedNamedEntitySet_ORG = []

    while (doCrossmatching_PER or doCrossmatching_ORG):
        (newCrossMatchedNamedEntities_PER
        ) = performCrossMatching(multipleMatchesNamedEntitySet_PER,singleMatchNamedEntitySet_ORG)
        if (len(newCrossMatchedNamedEntitySet_PER) == 0 ):
            doCrossmatching_PER = False
        newCrossMatchedNamedEntitySet_PER = newCrossMatchedNamedEntities_PER    
        crossMatchedNamedEntitySet_PER = crossMatchedNamedEntitySet_PER.union(newCrossMatchedNamedEntitySet_PER)
        singleMatchNamedEntitySet_PER = singleMatchNamedEntitySet_PER.union(newCrossMatchedNamedEntitySet_PER)
        multipleMatchesNamedEntitySet_PER = multipleMatchesNamedEntitySet_PER.difference(newCrossMatchedNamedEntitySet_PER)
        
        (newCrossMatchedNamedEntities_ORG
        ) = performCrossMatching(multipleMatchesNamedEntitySet_ORG,singleMatchNamedEntitySet_PER)
        if (len(newCrossMatchedNamedEntitySet_ORG) == 0):
            doCrossmatching_ORG = False
        newCrossMatchedNamedEntitySet_ORG = newCrossMatchedNamedEntities_ORG
        crossMatchedNamedEntitySet_ORG = crossMatchedNamedEntitySet_ORG.union(newCrossMatchedNamedEntitySet_ORG)
        singleMatchNamedEntitySet_ORG = singleMatchNamedEntitySet_ORG.union(newCrossMatchedNamedEntitySet_ORG)
        multipleMatchesNamedEntitySet_ORG = multipleMatchesNamedEntitySet_ORG.difference(newCrossMatchedNamedEntitySet_ORG)
# 
#     # Names with exactly one match
#     singleMatched_PER = [(oneKey_hashesAndEntityDataPair[0],oneKey_hashesAndEntityDataPair[1][1]) for oneKey_hashesAndEntityDataPair in singleMatchNamedEntities_PER]
#     singleMatched_ORG = [(oneKey_hashesAndEntityDataPair[0],oneKey_hashesAndEntityDataPair[1][1]) for oneKey_hashesAndEntityDataPair in singleMatchNamedEntities_ORG]
#     crossmatched_PER = [(oneKey_hashesAndEntityDataPair[0],oneKey_hashesAndEntityDataPair[1][1]) for oneKey_hashesAndEntityDataPair in crossmatched_PER]
#     crossmatched_ORG = [(oneKey_hashesAndEntityDataPair[0],oneKey_hashesAndEntityDataPair[1][1]) for oneKey_hashesAndEntityDataPair in crossmatched_ORG]
#     
#     # Names with two or more matches
#     noncrossmatched_PER = [(oneKey_hashesAndEntityDataPair[0]) for oneKey_hashesAndEntityDataPair in noncrossmatched_PER]
#     noncrossmatched_ORG = [(oneKey_hashesAndEntityDataPair[0]) for oneKey_hashesAndEntityDataPair in noncrossmatched_ORG]
#     
#     # Names with no matches
#     notmatchable_PER = [(oneKey_hashesAndEntityDataPair[0]) for oneKey_hashesAndEntityDataPair in nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList_PER]
#     notmatchable_ORG = [(oneKey_hashesAndEntityDataPair[0]) for oneKey_hashesAndEntityDataPair in nonCrossmatchable_key_listOfHashesAndEntityDataPairs_PairList_ORG]
#     notmatchable_PER = filter(lambda oneName : oneName not in noncrossmatched_PER, notmatchable_PER) # Otherwise includes noncrossmatched
#     notmatchable_ORG = filter(lambda oneName : oneName not in noncrossmatched_ORG, notmatchable_ORG)
#     crossmatched_nameOnly_PER = [onePair[0] for onePair in crossmatched_PER]
#     crossmatched_nameOnly_ORG = [onePair[0] for onePair in crossmatched_ORG]
#     notmatchable_PER = filter(lambda oneName : oneName not in crossmatched_nameOnly_PER, notmatchable_PER) # Otherwise includes crossmatched
#     notmatchable_ORG = filter(lambda oneName : oneName not in crossmatched_nameOnly_ORG, notmatchable_ORG)
    
    singleMatched_PER = list(singleMatchNamedEntitySet_PER.intersection(crossMatchedNamedEntitySet_PER))
    crossmatched_PER = list(crossMatchedNamedEntitySet_PER)
    noncrossmatched_PER = list(multipleMatchesNamedEntitySet_PER)
    singleMatched_ORG = list(singleMatchNamedEntitySet_ORG.intersection(crossMatchedNamedEntitySet_ORG))
    crossmatched_ORG = list(crossMatchedNamedEntitySet_ORG)
    noncrossmatched_ORG = list(multipleMatchesNamedEntitySet_ORG)
    
    # Add results to result dictionary 
    onePageDict["singleMatchedEntities_PER"] = singleMatched_PER
    onePageDict["crossmatchedEntities_PER"] = crossmatched_PER
    onePageDict["nonCrossmatchedNames_PER"] = noncrossmatched_PER
    #onePageDict["notMatchableNames_PER"] = notmatchable_PER

    onePageDict["singleMatchedEntities_ORG"] = singleMatched_ORG
    onePageDict["crossmatchedEntities_ORG"] = crossmatched_ORG
    onePageDict["nonCrossmatchedNames_ORG"] = noncrossmatched_ORG
    #onePageDict["notMatchableNames_ORG"] = notmatchable_ORG

    del onePageDict["matchfulNamedEntities_PER"]
    del onePageDict["matchfulNamedEntities_ORG"]
    
    return onePageDict
            
def retrieveMatchingKnownEntities(matchfulNamedEntities,normalizedName_EntityData_Dict_broadcast):
    """
    For each NamedEntity in matchfulNamedEntities, for each key in NamedEntity.matchingKnownEntityKeys, 
        retrieve the corresponding entity data pair (PER-ORGrels or ORG-PERrels) from 
        normalizedName_EntityData_Dict_broadcast.
    Returns single-match NamedEntity list, multi-match NamedEntity list, 
        both with added NamedEntity.matchingKnownEntities values.
    """            
    normalizedName_EntityData_Dict = normalizedName_EntityData_Dict_broadcast.value
    singleMatchNamedEntities = []
    multipleMatchesNamedEntities = []
    
    for oneMatchfulNamedEntity in matchfulNamedEntities:
        matchingKnownEntities = []
        ## for every key, get value from dict
        ## for every value, get every known entity data block (xxdata,[yyreldata])
        for oneMatchfulKey in oneMatchfulNamedEntity.matchingKnownEntityKeys:
            xx_yyRelsList = list(normalizedName_EntityData_Dict[oneMatchfulKey]) ##  [(perdata,[orgreldata])]
            for one_xx_yyRels in xx_yyRelsList:
                matchingKnownEntities.append(one_xx_yyRels)
        oneMatchfulNamedEntity.matchingKnownEntities = list(set(matchingKnownEntities))  ## Ignore duplicates  
        if len(matchingKnownEntities) == 1: ## A single known entity matches the key
            singleMatchNamedEntities.append(oneMatchfulNamedEntity)
        elif len(xx_yyRelsList) > 1: ## Multiple known entities match the key
            multipleMatchesNamedEntities.append(oneMatchfulNamedEntity)
    return singleMatchNamedEntities, multipleMatchesNamedEntities


def performCrossMatching(multipleMatchesNamedEntities,otherTypeSingleMatchNamedEntities):
    """
    Checks if multiple possibilities for matching entities can be narrowed down by PER-ORG data.
    If a common relation exists between an ORG entity and any entity from PER (or vice versa), a cross-matching possibility exists.
    If no matching relations are found, then it is not possible to perform cross-matching.
    Returns list of successfully cross-matched NamedEntities.
    """
    #TODO: create somewhere else for less re-calculation when looping?
    otherTypeSet = set()
    for oneOtherTypeSingleMatchNamedEntity in otherTypeSingleMatchNamedEntities:
        oneMatchingKnownEntity = oneOtherTypeSingleMatchNamedEntity.matchingKnownEntities[0] ## (id,...)
        oneKnownEntityIdentifier = oneMatchingKnownEntity[0]
        otherTypeSet.add(oneKnownEntityIdentifier)
    
    crossMatchedNamedEntities = []
    for oneMultipleMatchesNamedEntity in multipleMatchesNamedEntities:
        crossMatches = []
        for oneKnownEntityCandidate in oneMultipleMatchesNamedEntity.matchingKnownEntities:
            xxdata = oneKnownEntityCandidate[0]
            yyRelDatasList = oneKnownEntityCandidate[1]
            for one_yyRelData in yyRelDatasList: #(id,...)
                if one_yyRelData[0] in otherTypeSet: # If id links to single match, crossmatch exists
                    newCrossMatch = (xxdata,[one_yyRelData])
                    crossMatches.append(newCrossMatch)
        if (len(crossMatches) == 1): # Matched to exactly one, success
            crossMatchedNamedEntities.append(oneMultipleMatchesNamedEntity)
        
    return crossMatchedNamedEntities


def crossReferenceMultiCandidateListToSingleMatchSet(multiCandidateXxList,singleMatchYySet, crossMatchedXxSet):
    """
    Cross-references known entity id values of multiCandidateXxList to singleMatchYySet.
    If a singular cross-reference exists, add to crossMatchedXxSet and remove from multiCandidateXxList.
    Returns (crossMatchedXxSet, multiCandidateXxList).
    """

    for multiCandidateXx in multiCandidateXxList:
        namedEntity = multiCandidateXx[0]
        xxYyRelPairList = multiCandidateXx[1]
        bestCandidate = None
        tooManyMatches = False
        for xxYyRelPair in xxYyRelPairList:
            xx = xxYyRelPairList[0]
            yyRelPairs = xxYyRelPairList[1]
            for yyRelPair in yyRelPairs:
                yyId = yyRelPair[0]
                for singleMatchYyPair in list(singleMatchYySet):
                    singleMatchYyId = singleMatchYyPair[1][0]
                    if (yyId == singleMatchYyId):
                        if (bestCandidate == None):
                            bestCandidate = yyRelPair
                        else:
                            tooManyMatches = True
                            break
                    if tooManyMatches:
                        break
                if tooManyMatches:
                    break
            if tooManyMatches:
                break
        if (not tooManyMatches):
            pair = (namedEntity, xx)
            crossMatchedXxSet.add(pair)
            multiCandidateXxList.remove(multiCandidateXx)
    return crossMatchedXxSet, multiCandidateXxList

def calculateShortestNameDist(normalizedName, names):
    """
    Calculates minimal edit distance between normalizedName and any name in names..
    Returns shortest summary distance found.
    """
    shortestDist = sys.maxint
    for oneName in names:
        dist = nltk.edit_distance(normalizedName, oneName, substitution_cost=1, transpositions=False)
        if (dist < shortestDist):
            shortestDist = dist
    return shortestDist
    
def deduplicateCandidates(candidateList):
    """
    Deduplicates candidates with identical ids.
    Longest key version is preserved.
    Input:  [( (id, xxName),[yyRelPairs],(oneKey,normalizedName)]
    Output: [( (id, xxName),[yyRelPairs],(oneKey,normalizedName)]
    """
    #NOTE: May wish to preserve more keys than one
    keyDict = dict()
    for oneCandidate in candidateList:
        oneId = oneCandidate[0][0]
        relPairsList = oneCandidate[1]
        key = oneCandidate[2][0]
        normalizedName = oneCandidate[2][1]
        if oneId in keyDict: # Same id candidate exists, check which has longer key
            existingCandidate = keyDict[oneId]
            existingKey = existingCandidate[2][0]
            if len(key) > len(existingKey): # This key is longer, preserve this one
               keyDict[oneId] = oneCandidate
            else:
                pass 
        else:
            keyDict[oneId] = oneCandidate
    returnList = [x for x in keyDict.itervalues()]
    return returnList

def regroupByName(namedEntity_keyNormalizedNameXxYyRelPairsList):
    """
    Extracts NamedEntity original name and reformats/groups candidates by that name.
    Input :  [ ( NamedEntity,  (key,normalizedname,[xxYyRelPairs])  ) ]
    Returns: [(name, [( (id, xxName),[yyRelPairs],(key,normalizedName))], NamedEntity ))]
    """
    
    # Result: (name, [(oneKey, normalizedName, xxYyRelPairs)], namedEntity)
    name_keyNormalizedNameXxYyRelPairsDict = dict()
    for onePair in namedEntity_keyNormalizedNameXxYyRelPairsList:
        namedEntity = onePair[0]
        name = namedEntity.name
        keyNormalizedNameXxYyRelPairsList = onePair[1]
        if name in name_keyNormalizedNameXxYyRelPairsDict:
            name_keyNormalizedNameXxYyRelPairsDict[name] += [keyNormalizedNameXxYyRelPairsList]
        else:
            name_keyNormalizedNameXxYyRelPairsDict[name] = [keyNormalizedNameXxYyRelPairsList]
    grouped_name_keyNormalizedNameXxYyRelPairs_namedEntity_TripleList = [(k,v,namedEntity) for k,v in name_keyNormalizedNameXxYyRelPairsDict.iteritems()] 
    
    name_idXxNameYyRelPairsKeyNormalizedName_namedEntity_List = []
    for oneTriple in grouped_name_keyNormalizedNameXxYyRelPairs_namedEntity_TripleList:
        name = oneTriple[0]
        keyNormalizedNameXxYyRelPairsList = oneTriple[1]
        namedEntity = oneTriple[2]
        idXxNameYyRelPairsKeyNormalizedName_TripleList = []
        for oneknxyrp in keyNormalizedNameXxYyRelPairsList:
            oneKey = oneknxyrp[0]
            normalizedName = oneknxyrp[1]
            xyrp = oneknxyrp[2]
            for onexyr in xyrp:
                idAndName = onexyr[0]
                yyRelPairs = onexyr[1]
                oneidXxNameTriple = (idAndName, yyRelPairs,(oneKey, normalizedName))
                idXxNameYyRelPairsKeyNormalizedName_TripleList.append(oneidXxNameTriple)
        oneNameTriple = (name, idXxNameYyRelPairsKeyNormalizedName_TripleList, namedEntity)
        name_idXxNameYyRelPairsKeyNormalizedName_namedEntity_List.append(oneNameTriple)
    return name_idXxNameYyRelPairsKeyNormalizedName_namedEntity_List
    
def preserveNearestCandidates(name_idXxNameYyRelPairsKeyNormalizedName_namedEntity_List, matchingDistanceThreshold=sys.maxint):
    """
    Returns list with only those candidates with the minimal edit distance between 
    candidate normalized name and any of NamedEntity normalized names.
    # HEURISTIC
    # Preserve only closest matches (by shortest edit distance between oneKey and named entity normalized name)
    Input :  [(name, [( (id, xxName),[yyRelPairs],(key,normalizedName))], NamedEntity ))]
    Returns: [(name, [( (id, xxName),[yyRelPairs],(key,normalizedName))], NamedEntity ))]
    """

    shortenedList = []
    for oneTriple in name_idXxNameYyRelPairsKeyNormalizedName_namedEntity_List:
        name = oneTriple[0]
        keyNormalizedNameXxYyRelPairsList = oneTriple[1]
        namedEntity = oneTriple[2]
        shortestDist = matchingDistanceThreshold
        preservedCandidates = []
        for oneCandidate in keyNormalizedNameXxYyRelPairsList:
            oneId = oneCandidate[0][0]
            relPairsList = oneCandidate[1]
            oneKey = oneCandidate[2][0]
            normalizedName = oneCandidate[2][1]
            dist = calculateShortestNameDist(normalizedName, 
                                             namedEntity.normalizedForms)
            if (dist < shortestDist):
                shortestDist = dist
                preservedCandidates = [oneCandidate]
            elif (dist == shortestDist):
                preservedCandidates.append(oneCandidate)
        # Deduplicate same-oneKey values
        preservedCandidates = deduplicateCandidates(preservedCandidates)
        shortenedList.append( (name, preservedCandidates, namedEntity) )
    return shortenedList


def separateByCandidateCount(name_idXxNameYyRelPairs_namedEntity_TripleList):
    """
    Separates input list based on number of matching candidates. 
    Returns singleMatchXxDict, multiCandidateXxList, noCandidateXxList.
    Input: [(name, [( (id, xxName),[yyRelPairs],(key,normalizedName))], NamedEntity ))]
    Returns: 
        singleMatchDict : {name : ((id, xxName),(key,normalizedName))}
        multiCandidateList : [(name, [( (id, xxName),[yyRelPairs],(key,normalizedName))])]
        noCandidateList : [(name, [( (id, xxName),[yyRelPairs],(key,normalizedName))])]
    """
    singleMatchXxDict = dict()
    multiCandidateXxList = []
    noCandidateXxList = []
    for oneTriple in name_idXxNameYyRelPairs_namedEntity_TripleList:
        name = oneTriple[0]
        candidateList = oneTriple[1]
        # if single match, take it
        if len(candidateList) == 1:
            val = (candidateList[0][0], candidateList[0][2])
            singleMatchXxDict[name] = val
        # if no matches, put to no match list
        elif len(candidateList) == 0:
            noCandidatePair = (name,candidateList)
            noCandidateXxList.append(noCandidatePair)          
        # if multiple candidates, put multi-candidate list
        else:
            multiCandidatePair = (name,candidateList)
            multiCandidateXxList.append(multiCandidatePair)
    return (singleMatchXxDict, multiCandidateXxList, noCandidateXxList)

def formatSingleMatchDictToOutput(inputDict):
    """
    Formats matched entities dictionary to outputtable list format.
    Input: {name : ((id, firstName, lastName),(key,normalizedName))}
    Returns: [(name,(id, firstName, lastName),(key,normalizedName))]
    """
    returnList = [(k,v[0],v[1]) for k,v in inputDict.iteritems()]
    return returnList

def formatMultiMatchListToOutput(inputList):
    """
    Formats multi-candidate unmatched entities list to outputtable list format.
    Input: [(name, [( (id, xxName),[yyRelPairs],(key,normalizedName))])]
    Returns: [ (name, [ (id, xxName),(key,normalizedName) ] ) ]
    """
    
    returnList = [(oneElem[0], removeRelsFromList(oneElem[1]) ) for oneElem in inputList]
    return returnList

def removeRelsFromList(candidateList):
    """
    Input:   [( (id, xxName),[yyRelPairs],(key,normalizedName))]
    Returns: [ (id, xxName),(key,normalizedName) ]
    """
    return [(oneTriple[0],oneTriple[2]) for oneTriple in candidateList]
    
def improveAndReorganizeMatching(sourceId_perListofHitsAndOrgListOfHits):
    """
    Extracts closest matches only.
    Performs cross-matching between multi-candidate entities of one type and single-match entities of other type.
    Reorganizes data to output format.
    
    Input:
    (sourceId, 
       (
        [ (normalizedPerName, ((NamedEntity, key),[perOrgRelPairs]) ) ],
        [ (normalizedOrgName, ((NamedEntity, key),[orgPerRelPairs]) ) ]
       )
    )
    Returns: dictionary with matching results
    """
    sourceId = sourceId_perListofHitsAndOrgListOfHits[0]
    perListOfHits = sourceId_perListofHitsAndOrgListOfHits[1][0]
    orgListOfHits = sourceId_perListofHitsAndOrgListOfHits[1][1]
    
    # Reorganize by (matchful) named entity
    # result: (NamedEntity, (key,normalizedName,[perOrgRelPairs]))
    namedEntity_keyNormalizedNamePerOrgRelPairsList = [(namedEntity, (key,normalizedname,perOrgRelPairsList)) for (normalizedname, ((namedEntity,key), perOrgRelPairsList) ) in perListOfHits ]
    namedEntity_keyNormalizedNameOrgPerRelPairsList = [(namedEntity, (key,normalizedname,orgPerRelPairsList)) for (normalizedname, ((namedEntity,key), orgPerRelPairsList) ) in orgListOfHits ]
 
    # Preserve best candidates (nearest edit distance)
    # Result : [(name, [( (id, xxName),[yyRelPairs],(key,normalizedName))], NamedEntity ))]
    distThreshold = distanceThresholdPer
    name_idPerNameOrgRelPairsKeyNormalizedName_namedEntity_List = regroupByName(namedEntity_keyNormalizedNamePerOrgRelPairsList)
    name_idPerNameOrgRelPairsKeyNormalizedName_namedEntity_FilteredList = preserveNearestCandidates(name_idPerNameOrgRelPairsKeyNormalizedName_namedEntity_List, matchingDistanceThreshold=distThreshold)
    distThreshold = distanceThresholdOrg
    name_idOrgNamePerRelPairsKeyNormalizedName_namedEntity_List = regroupByName(namedEntity_keyNormalizedNameOrgPerRelPairsList)
    name_idOrgNamePerRelPairsKeyNormalizedName_namedEntity_FilteredList = preserveNearestCandidates(name_idOrgNamePerRelPairsKeyNormalizedName_namedEntity_List, matchingDistanceThreshold=distThreshold)

    # Create data structures for matchfulness categories (zero, one, more than one candidate)                
    #{name : ((id, firstName, lastName),(key,normalizedName))}
    singleMatchPerDict = dict()
    #{name : ((id, orgName),(key,normalizedName))}
    singleMatchOrgDict = dict()
    #[(name, [( (id,firstName,lastName),[orgRelPairs],(key,normalizedName))])]
    multiCandidatePerList = []
    #[(name, [])]
    noCandidatePerList = []
    #[(name, [( (id, orgName),[perRelPairs],(key,normalizedName))])]
    multiCandidateOrgList = []
    #[(name, [])]
    noCandidateOrgList = []
    
    # Populate single-match and multi-candidate lists
    (singleMatchPerDict,multiCandidatePerList,noCandidatePerList) = separateByCandidateCount(name_idPerNameOrgRelPairsKeyNormalizedName_namedEntity_FilteredList)
    (singleMatchOrgDict,multiCandidateOrgList,noCandidateOrgList) = separateByCandidateCount(name_idOrgNamePerRelPairsKeyNormalizedName_namedEntity_FilteredList)

 
    #DEBUG
#     returnDict = dict()
#     returnDict['pageId'] = sourceId
#     returnDict['debug_single'] = [(k,v) for k,v in singleMatchPerDict.iteritems()]
#     returnDict['debug_x'] = name_idPerNameOrgRelPairsKeyNormalizedName_namedEntity_FilteredList
#     return returnDict
    #ENDEBUG

    #TODO: implement
    # Cross-reference other-type IDs to narrow down results
    crossMatchedPerDict = dict()
    crossMatchedOrgDict = dict()
    
#     nonMatchedPerCount = len(multiCandidatePerList)
#     nonMatchedOrgCount = len(multiCandidateOrgList)
#     keepCrossMatching = True
#     while (keepCrossMatching):
#         crossMatchedPerDict, multiCandidatePerList = crossReferenceMultiCandidateListToSingleMatchSet(multiCandidatePerList,
#                                                                    singleMatchOrgDict.union(crossMatchedOrgDict), 
#                                                                    crossMatchedPerDict)
#         crossMatchedOrgDict, multiCandidateOrgList = crossReferenceMultiCandidateListToSingleMatchSet(multiCandidateOrgList,
#                                                                    singleMatchPerDict.union(crossMatchedPerDict),
#                                                                    crossMatchedOrgDict)
#         # If no new singular matches were found this cycle, stop
#         if (len(multiCandidatePerList)== nonMatchedPerCount and len(multiCandidateOrgList) == nonMatchedOrgCount):
#             keepCrossMatching = False
#     
#     # Unable to narrow down multi-candidates, leave them unmatched
#     #TODO: remove candidates from final output
#     nonCrossMatchedPerList = multiCandidatePerList
#     nonCrossMatchedOrgList = multiCandidateOrgList
#     

    #DEBUG
    nonCrossMatchedPerList = []
    if len(multiCandidatePerList) > 0:
        aa = multiCandidatePerList.pop()
        nonCrossMatchedPerList.append( (aa[0],list(aa[1])) )
    nonCrossMatchedOrgList = []
    if len(multiCandidateOrgList) > 0:
        aa = multiCandidateOrgList.pop()
        nonCrossMatchedOrgList.append( (aa[0],list(aa[1])) )
    #ENDEBUG
    
    
    #TODO: create proper output
    # Format output
    returnDict = dict()
    returnDict['pageId'] = sourceId
    returnDict['singleMatchedEntities_PER'] = formatSingleMatchDictToOutput(singleMatchPerDict)
    returnDict['singleMatchedEntities_ORG'] = formatSingleMatchDictToOutput(singleMatchOrgDict)
    returnDict['crossmatchedEntities_PER'] = formatSingleMatchDictToOutput(crossMatchedPerDict)
    returnDict['crossmatchedEntities_ORG'] = formatSingleMatchDictToOutput(crossMatchedOrgDict)
    returnDict['nonCrossmatchedNames_PER'] = formatMultiMatchListToOutput(nonCrossMatchedPerList)
    returnDict['nonCrossmatchedNames_ORG'] = formatMultiMatchListToOutput(nonCrossMatchedOrgList)
    returnDict['noCandidateNames_PER'] = formatMultiMatchListToOutput(noCandidatePerList)
    returnDict['noCandidateNames_ORG'] = formatMultiMatchListToOutput(noCandidateOrgList)
#     returnDict['distinctNames']
#     returnDict['nameLabelPairs']
#     returnDict['excludedNames']
#     returnDict['matchlessNames']
#     returnDict['notMatchableNames_PER']
#     returnDict['notMatchableNames_ORG']

    return returnDict

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
        hostname = inputDict["hostname"]
        path = inputDict["path"]
        date = inputDict["date"]
        elementList = inputDict[categoryName]
        return [hostname + "::" + path + "::" + date + "::" + str(oneElement) for oneElement in elementList]
    except:
        return None
    
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
        
def textifyKeys_PER(normalizedxxName_xxYyRelPairs):
    #TODO: REMOVE (DEPRECATED)
    """
    Returns (id,firstname, lastname)[()] format as plaintext.
    """
    outString = ""
    key = normalizedxxName_xxYyRelPairs[0]
    value = normalizedxxName_xxYyRelPairs[1]
    id_PER = value[0][0]
    firstName_PER = value[0][1]
    lastName_PER = value[0][2]
    outString = u",".join([id_PER,firstName_PER,lastName_PER])
    #outString = str(key) + "," + id_PER +","+firstName_PER+","+lastName_PER+","+str(len(hashes))
    return outString

def textifyKeys_ORG(normalizedxxName_xxYyRelPairs):
    #TODO: REMOVE (DEPRECATED)
    """
    Returns (id,orgname)[()] format as plaintext.
    """
    outString = ""
    key = normalizedxxName_xxYyRelPairs[0]
    value = normalizedxxName_xxYyRelPairs[1]
    id_ORG = value[0][0]
    name_ORG = value[0][1]
    outString = u",".join([id_ORG,name_ORG])
    #outString = str(key) + "," + id_ORG +","+name_ORG+","+str(len(hashes))
    return outString

def processKnownEntitiesCsvFile():
    """
    Reads known entity data from CSV file and creates a dictionary. 
    For each data line, two key-value pairs (PER and ORG) are created with normalized name as key and line data dictionary as value.
    Lines that share a normalized name are grouped under the same key.
    """
    print("Processing known entity data csv file...")
    knownEntityProcessingStartTime = time.time()
    # load file with known entity data (person + organization data rows)
    knownEntitiesFile = spark.textFile(knownEntityPath,use_unicode=True)

    # parse known entity file lines into tuples
    # filter out invalid lines
    # result: DataLine named tuple RDD
    knownEntityDataLine_RDD = knownEntitiesFile.map(lambda line: parseEntities(line)).filter(lambda entry: entry is not None)
    knownEntityDataLine_RDD.cache()
 
    # Extract person info from known entity named tuples
    # Group ORG data by PER (each person has a dict of related ORGs)
    # result pair : ((id_PER, firstName_PER, lastName_PER), (id_ORG,name_ORG,startDate,endDate,relation))
    perData_orgRelData_PairRDD = knownEntityDataLine_RDD.map(lambda entity: extractPerPairFromKnownEntityDataLine(entity) )
    perData_orgRelData_PairRDD = perData_orgRelData_PairRDD.filter(lambda entity : entity is not None)
    perData_orgRelData_PairRDD = perData_orgRelData_PairRDD.filter(lambda entity : len(entity[0][1]) > 0 and len(entity[0][2]) > 0) # Exclude names with no firstname or lastname
    # result pair : ((id_PER, firstName_PER, lastName_PER), [(id_ORG,name_ORG,startDate,endDate,relation)])
    perData_groupedOrgRelData_PairRDD = perData_orgRelData_PairRDD.groupByKey()
    perData_orgRelData_PairRDD.unpersist()
    # NOTE: may be useful to create dict by id_ORG for quicker cross-referencing
    # Normalize name for PER matching
    # result pair : (normalizedPerName, [ ((id_PER, firstName_PER, lastName_PER), [(id_ORG,name_ORG,startDate,endDate,relation)])])
    normalizedPerName_perOrgRelPairs_PairRDD = perData_groupedOrgRelData_PairRDD.map(lambda entity : (normalizeName(u" ".join([ entity[0][1],entity[0][2] ]),removeShortWords=False,doSynonymReplace=False), [entity]) )
    normalizedPerName_perOrgRelPairs_PairRDD = normalizedPerName_perOrgRelPairs_PairRDD.reduceByKey(lambda x,y: x+y, numPartitions=nrOfPartitions )
    # Generate n-gram keys from normalized name, group by n-gram
    # result pair : (ngramKey, [normalizedPerName])
    ngramKey_normalizedPerNames_PairRDD = normalizedPerName_perOrgRelPairs_PairRDD.flatMap(lambda entity : createNgramKeyNamePairs(entity[0], int(len(entity[0])*ngramLengthThresholdMult)))
    ngramKey_normalizedPerNames_PairRDD = ngramKey_normalizedPerNames_PairRDD.reduceByKey(lambda x,y: x+y, numPartitions=nrOfPartitions )
    ngramKey_normalizedPerNames_PairRDD = ngramKey_normalizedPerNames_PairRDD.map(lambda x : (x[0],list(set(x[1]))))
    # Extract organization info from known entity lines
    # result pair : ((id_ORG, name_ORG), (id_PER,firstName_PER,lastName_PER,startDate,endDate,relation))
    orgData_perRelData_PairRDD = knownEntityDataLine_RDD.map(lambda entity: extractOrgPairFromKnownEntityDataLine(entity))
    orgData_perRelData_PairRDD = orgData_perRelData_PairRDD.filter(lambda entity : entity is not None)
    orgData_perRelData_PairRDD = orgData_perRelData_PairRDD.filter(lambda entity : entity[0][1] is not None)
    # result pair : ((id_ORG, name_ORG), [(id_PER,firstName_PER,lastName_PER,startDate,endDate,relation)])
    orgData_groupedPerRelData_PairRDD = orgData_perRelData_PairRDD.groupByKey()
    # Normalize name for ORG matching
    #TODO: rework known ORG lemmatization (is it even needed?)
    if flag_lemmatizeKnownNames: # If enabled, create additional alternative keys for known entities
    # result pair : ((id_ORG, altname_ORG), [(id_PER,firstName_PER,lastName_PER,startDate,endDate,relation)])
        lemmatizedOrgName_groupedOrgPerRelData_PairRDD = orgData_groupedPerRelData_PairRDD.flatMap(lambda pair : lemmatizeAndPairWithData(pair, type="org"))
    
    # result pair : (normalizedOrgName, [ ((id_ORG,name_ORG),[(id_PER,firstName_PER,lastName_PER,startDate,endDate,relation)] ) ])
    normalizedOrgName_orgPerRelPairs_PairRDD = orgData_groupedPerRelData_PairRDD.map(lambda entity : (normalizeName( unicode(entity[0][1]),removeShortWords=False,doSynonymReplace=True ), [entity] ))
    normalizedOrgName_orgPerRelPairs_PairRDD = normalizedOrgName_orgPerRelPairs_PairRDD.reduceByKey(lambda x,y: x+y, numPartitions=nrOfPartitions )
    # Generate n-gram keys from normalized name, group by n-gram
    # result pair : (ngramKey, [normalizedPerName])
    ngramKey_normalizedOrgNames_PairRDD = normalizedOrgName_orgPerRelPairs_PairRDD.flatMap(lambda entity : createNgramKeyNamePairs(entity[0],int(len(entity[0])*ngramLengthThresholdMult)))
    ngramKey_normalizedOrgNames_PairRDD = ngramKey_normalizedOrgNames_PairRDD.reduceByKey(lambda x,y: x+y, numPartitions=nrOfPartitions)
    ngramKey_normalizedOrgNames_PairRDD = ngramKey_normalizedOrgNames_PairRDD.map(lambda x : (x[0],list(set(x[1]))))
    
    if flag_doKnownEntityOutput: # If enabled, output entity and keys to file
        normalizedOrgName_orgPerRelPairs_PairRDD.saveAsTextFile(outputPath + "/known_ORG")

    if flag_doAllKnownEntityKeysOutput: ## if true, output all matchable keys to files
        normalizedPerName_perOrgRelPairs_PairRDD.groupByKey().map(lambda one_normalizedName_perOrgRelPairs : textifyKeys_PER(one_normalizedName_perOrgRelPairs) ).saveAsTextFile(outputPath+"/data_PER")
        normalizedOrgName_orgPerRelPairs_PairRDD.groupByKey().map(lambda one_normalizedName_orgPerRelPairs : textifyKeys_ORG(one_normalizedName_orgPerRelPairs) ).saveAsTextFile(outputPath+"/data_ORG")
    knownEntityProcessingEndTime = time.time()
    return (normalizedPerName_perOrgRelPairs_PairRDD, normalizedOrgName_orgPerRelPairs_PairRDD, ngramKey_normalizedPerNames_PairRDD, ngramKey_normalizedOrgNames_PairRDD)

def main():
    """
    Executes NER and known entity matching.
    """
    #DEBUG
#     debugname = u"Tartu Ülikool"
#     print("DEBUG ngram: " + unicode(createNgramKeyNamePairs(debugname, int(ngramLengthThresholdMult * len(debugname)))))
#     input("WAITING")
    #ENDEBUG
    
    if outputPath.startswith("hdfs:"):
        if call(["hadoop", "fs", "-test", "-d",outputPath]) == 0:
            print("Output directory already exists. Stopping.")
            sys.exit("Output directory already exists.")     
    elif os.path.isdir(outputPath):
        print("Output directory already exists. Stopping.")
        sys.exit("Output directory already exists.")    
        
    conf = SparkConf()
    global spark
    spark = SparkContext(appName="NER_matcher", conf=conf)
    
    ################
    # Exclusions and synonyms: Process exclusion/synonym files, create exclusion and synonym broadcasts
    ################
    print("Processing exclusion and synonym file...")
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
    #excludeSet = excludeSet.difference(ngramKey_normalizedOrgNames_broadcast.value)
    
    # Broadcast exclusions
    global excludeSet_broadcast
    excludeSet_broadcast = spark.broadcast(excludeSet)
    global excludeSet_nonNormalized_broadcast 
    excludeSet_nonNormalized_broadcast = spark.broadcast(nonNormalizedExcludeSet)
    
    # Broadcast synonyms
    global synonymDict_broadcast
    synonymDict_broadcast = spark.broadcast(synonymDict)
    
    print ("EXCLUDESET LEN : " + str(len(excludeSet_broadcast.value)))
    print ("SYNONYMDICT LEN : " + str(len(synonymDict_broadcast.value)))

    ################
    #  Process known entity data into matchable names
    ################
    print ("Processing known entities...")
    if flag_processKnownEntities: # Read from CSV, process lines, save to Pickle file
        call(["hadoop", "fs", "-rm", "-r", "-skipTrash", knownEntityPath + "/KEDict_PER"]) # delete previous version
        call(["hadoop", "fs", "-rm", "-r", "-skipTrash", knownEntityPath + "/KEDict_ORG"])
        call(["hadoop", "fs", "-rm", "-r", "-skipTrash", knownEntityPath + "/KEngram_PER"])
        call(["hadoop", "fs", "-rm", "-r", "-skipTrash", knownEntityPath + "/KEngram_ORG"])
        (normalizedPerName_perOrgRelPairs_PairRDD, normalizedOrgName_orgPerRelPairs_PairRDD,
         ngramKey_normalizedPerNames_PairRDD, ngramKey_normalizedOrgNames_PairRDD) = processKnownEntitiesCsvFile()
        normalizedPerName_perOrgRelPairs_PairRDD.saveAsPickleFile(knownEntityPath + "/KEDict_PER")
        normalizedOrgName_orgPerRelPairs_PairRDD.saveAsPickleFile(knownEntityPath + "/KEDict_ORG")
        ngramKey_normalizedPerNames_PairRDD.saveAsPickleFile(knownEntityPath + "/KEngram_PER")
        ngramKey_normalizedOrgNames_PairRDD.saveAsPickleFile(knownEntityPath + "/KEngram_ORG")
    else: # Read processed data from Pickle file
        normalizedPerName_perOrgRelPairs_PairRDD = spark.pickleFile(knownEntityPath + "/KEDict_PER")
        normalizedOrgName_orgPerRelPairs_PairRDD = spark.pickleFile(knownEntityPath + "/KEDict_ORG")
        ngramKey_normalizedPerNames_PairRDD = spark.pickleFile(knownEntityPath + "/KEngram_PER")
        ngramKey_normalizedOrgNames_PairRDD = spark.pickleFile(knownEntityPath + "/KEngram_ORG")
    print ("Known entities have been processed to RDDs.")
    print ("NGRAMMAP_PER SIZE:" + unicode(ngramKey_normalizedPerNames_PairRDD.count()))
    print ("NGRAMMAP_ORG SIZE:" + unicode(ngramKey_normalizedOrgNames_PairRDD.count()))


    #DEBUG
#     print(ngramKey_normalizedPerNames_PairRDD.filter(lambda x : x[0] == "ar kari").collect())
#     print(ngramKey_normalizedPerNames_PairRDD.filter(lambda x : x[0] == "lar kari").collect())
#     print(ngramKey_normalizedPerNames_PairRDD.filter(lambda x : x[0] == "lar karis").collect())
    print(ngramKey_normalizedPerNames_PairRDD.filter(lambda x : x[0] == "tiina vaino").collect())
    input("WAITING")
    #ENDEBUG


    ################
    # Exclusions : Exclude lone first names of known entities
    ################
    #TODO: move to known entity extraction (has to be done before ngram-normalizedname pairs?)
    #TODO: reenable
#     print("Excluding first names...")
#     names = ngramkey_normalizedPerNames_broadcast.value
#     firstnames = [oneName.split(" ")[0] for oneName in names if oneName != None]
#     firstnameSet = set(firstnames)
#     excludeSet = excludeSet.union(firstnameSet)
#     # Broadcast exclusions
#     excludeSet_broadcast = spark.broadcast(excludeSet)
#     

    ################
    # Parse manifest of unknown named entity input files
    ################
    inputPathString = ""
    manifestFile = open(manifestPath,'r')
    for line in manifestFile:
        inputPathString = inputPathString + line.strip() +","
    inputPathString = inputPathString.strip(",")
    
    ################
    # Process unknown named entities
    ################
    print("Processing unknown-named-entity input files...")
    oneFileStartTime = time.time()
    # extract Named Entity Recognition results from file
    nerResultsFile = spark.textFile(inputPathString,use_unicode=True)
    # each result element: dictionary with ['named_entities'],['hostname'],['path'],['date']
    # filter out invalid (empty) results
    nerResultDictRDD = nerResultsFile.map(lambda line: parseData(line)).filter(lambda entry: entry is not None)
    # create NamedEntity objects from ['named_entities']
    # result: RDD of dictionaries with ['normalizedNamedEntities'],['distinctNames'],['named_entities'],['pageId']
    #     ['normalizedNamedEntities'] - list of NamedEntity objects with 'name', 'normalizedForms', 'ngramKeys'
    NerResultsDictsWithNamedEntitiesRDD = nerResultDictRDD.map(lambda nerResult : createNamedEntityObjectsFromNames(nerResult))

    # Rearrange data to enable key-to-key matching
    # result: (NamedEntity,dict) pairs 
    namedEntity_nerResultsDict_PairRDD = NerResultsDictsWithNamedEntitiesRDD.flatMap(lambda oneDict : createNamedEntityAndDictPairs(oneDict))
    #TODO: ignore names in exclusion set
    namedEntity_nerResultsDict_PairRDD = namedEntity_nerResultsDict_PairRDD.filter(lambda x: (x[0].name not in excludeSet_nonNormalized_broadcast.value) and (x[0].name not in excludeSet_broadcast.value))
    
    # Reformat to ngram key
    # result: (key, NamedEntity) pairs
    key_namedEntity_PairRDD = namedEntity_nerResultsDict_PairRDD.flatMap(lambda oneElement : createKeyAndNamedEntityPairs(oneElement[0]))
    key_namedEntity_PairRDD.persist()
    
    ################
    # Matching : Find matching candidates for each named entity from among known entities.
    #     Uses combined n-gram keys for candidate creation. 
    ################
    # Returns: Dictionary with ["pageId"],["distinctNames"],
    #   ["matchfulNamedEntities_PER"],["matchfulNamedEntities_ORG"],
    #   ["matchlessNames"],["excludedNames"]
    ################
    
    # Join named entities and known entities by key
    # result: (key, (NamedEntity,[normalizedname]))
    key_NamedEntityAndNormNameList_PairRDD_PER = key_namedEntity_PairRDD.leftOuterJoin(ngramKey_normalizedPerNames_PairRDD)
    key_NamedEntityAndNormNameList_PairRDD_ORG = key_namedEntity_PairRDD.leftOuterJoin(ngramKey_normalizedOrgNames_PairRDD)
    
    # Retrieve known entity data (candidates) for each normalizedname
    # Group data by named entity source location (sourceId)
    # result: (sourceId, [(normalizedname, ((NamedEntity, key), [xxYyRelPairs]) )])
    sourceId_perListOfHits_PairRDD = collectDataBySourceId(key_NamedEntityAndNormNameList_PairRDD_PER,normalizedPerName_perOrgRelPairs_PairRDD)
    sourceId_orgListOfHits_PairRDD = collectDataBySourceId(key_NamedEntityAndNormNameList_PairRDD_ORG,normalizedOrgName_orgPerRelPairs_PairRDD)

    # join PER and ORG hit lists by sourceId
    sourceId_perListOfHitsAndOrgListOfHits_PairRDD = sourceId_perListOfHits_PairRDD.join(sourceId_orgListOfHits_PairRDD)

    # Extract closest matches only
    # Perform cross-matching
    # Convert to required output format
    outputDictsRDD = sourceId_perListOfHitsAndOrgListOfHits_PairRDD.map(lambda entity : improveAndReorganizeMatching(entity))
    
#     #DEBUG
#     print(outputDictsRDD.take(10))
#     inputLine = input("Waiting!")
#     #ENDEBUG
     
    outputDictsRDD.saveAsTextFile(outputPath + "/entityData")
    spark.stop()
    
#     endTime = time.time()
#     print "Time to finish:" + str(endTime - startTime)
#     
#     matchedNerResultsRDD = NerResultsDictsWithNamedEntitiesRDD.map(lambda oneResult : performKeyMatching(oneResult,ngramkey_normalizedPerNames_broadcast,ngramKey_normalizedOrgNames_broadcast))
#     NerResultsDictsWithNamedEntitiesRDD.unpersist()
#     
#     if flag_doMatchingOutput: ## Outputs matching results (matching and unmatching keys)
#         matchedNerResultsRDD.saveAsTextFile(outputPath + "/matching")
# 
#     ################
#     # Matching named entity retrieval : 
#     #  if a single candidate, retrieve it
#     #  if multiple candidates, search for PER-ORG line hashes that match 
#     ################
#     print("Retrieving matched entity data...")
#     entityDataRDD = matchedNerResultsRDD.map(lambda oneResult : performDataRetrieval(oneResult,normalizedPerName_perOrgRelPairs_broadcast,normalizedOrgName_orgPerRelPairs_broadcast))
#     
#     # Save result
#     entityDataRDD.saveAsTextFile(outputPath + "/entityData")
# 
#     ###############
#     # (optional) Exploded output of names:
#     #  if enabled, output each name of each page on separate line with URI
#     ###############
#     if flag_explodeOutputForNames:
#         explodedNamesRDD = matchedNerResultsRDD.flatMap(lambda oneResult : explodeOutputForEachName(oneResult))
#         explodedNamesRDD.saveAsTextFile(outputPath + "/explodedNames")
#     
#     ###############
#     # (optional) Exploded output of results:
#     #  if enabled, output each successful matching on separate line with URI
#     ###############
#     if flag_explodeOutputForSuccessfulMatches:
#         for categoryName in ["singleMatchedEntities_PER" ,"singleMatchedEntities_ORG","crossmatchedEntities_PER","crossmatchedEntities_ORG" ]:
#             explodedSuccessesRDD = matchedNerResultsRDD.flatMap(lambda oneResult : explodeOutputForEachInCategory(oneResult,categoryName)).filter(lambda onResult : oneResult is not None)
#             explodedSuccessesRDD.saveAsTextFile(outputPath + "/explodedSuccesses_"+categoryName)
#         
#     matchedNerResultsRDD.unpersist()
#     print("Finished unknown entity files.")
#     oneFileEndTime = time.time()
#     print("Time taken: " + str(oneFileEndTime - oneFileStartTime))
#     entityDataRDD.unpersist()
#     spark.stop()
#     endTime = time.time()
#     print "Time to finish:" + str(endTime - startTime)

if __name__ == "__main__":
    flag_doAllKnownEntityKeysOutput = False ## if True, output the lists of all known entity keys to data_PER and data_ORG directories 
    flag_ignoreNamedEntityLabels = True ## if True, ignore PER and ORG tags found from NLTK
    flag_doMatchingOutput = True ## If True, output matched/unmatched keys to "/matching" directory
    flag_doLabelPairOutput = True ## If True, output "name_label_pairs" into main output file
    flag_doKnownEntityOutput = False ## If True, output known entity name and key values to "/known_<type>" directory
    flag_explodeAlternatives = True ## If True, explode "|"-separated alternative name forms to separate names
    flag_outputExplodedAlternatives = True ## If True, output NLTK-detected names in exploded form. Otherwise, use "|"-separated form. 
    flag_explodeOutputForNames = True ## If True, output into "explodedNames" directory each name on a separate line with page URI
    flag_explodeOutputForSuccessfulMatches = False ## If True, output into "explodedResults_<categoryName>" each successful match on a separate line with page URI
    flag_lemmatizeKnownNames = False # If True, apply lemmatization to known names before normalizing
    flag_processKnownEntities = False # If True, read from CSV file and process known entity data. Otherwise assume it has been done and read from knownEntityDir.
    flag_removeSubnames = True # If True, ignore each name that is a substring of another name in the same URI location. 

    # Word-replacament dictionary to use in name normalization
    orgtypeReplaceDict = {u"aktsiaselts":u"as", u"korteriühistu":u"kü", u"osaühing":u"oü", u"mittetulundusühing":u"mtü", u"sihtasutus":u"sa"}
    
    inputEncoding = "utf-8"
    lemmatizedNamePartMultiplier = 0.8 # Multiplier for name part threshold in known entity name lemmatization
    ngramLevel = 10 # Length of N-gram tokens in N-gram generation
    nrOfPartitions = 200 # Number of partitions to partition to with repartition()
    ngramLengthThresholdMult = 0.8 # In n-gram key generation, omit keys shorter than this fraction of the input name
    # List of exclusion file tags: exclude tagged names from unknown name matching 
    distanceThresholdPer = 50 # Maximum allowed distance between ngram and matched normalized name
    distanceThresholdOrg = 50 # Maximum allowed distance between ngram and matched normalized name
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
    
    main()
