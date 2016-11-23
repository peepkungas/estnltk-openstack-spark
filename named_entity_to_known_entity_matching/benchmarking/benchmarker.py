#!/usr/bin/python
# -*- coding: utf-8 -*- 

import ast
import csv

class csvRow():
    pass

def parseData(line):
    y = ast.literal_eval(line.decode("raw_unicode_escape","replace"))
    #y = ast.literal_eval(line.decode("utf-8"))
    return y

def explodeValues(lineDict,lineDictKey,matchDict):
    if ("hostname" in lineDict.keys() and "path" in lineDict.keys() and "date" in lineDict.keys()):
        lineKey = lineDict["hostname"] + "::" + lineDict["path"] + "::" + lineDict["date"]
    if ("pageId" in lineDict.keys()):
        pageId = lineDict["pageId"]
        parts = pageId.split("::")
        lineKey = parts[1] + "::" + parts[2] + "::" +parts[4]
    nameKey = None
    nameValue = None
    outList = []
    for valblock in lineDict[lineDictKey]:
        #print valblock
        if len(valblock[1]) == 3:
            namePart = valblock[1][1] + " " + valblock[1][2]
        elif len(valblock[1]) == 2:
            namePart = valblock[1][1]
        else:
            continue
        outList.append((valblock[1][0],namePart))
    if len(outList)>0:
        matchDict[lineKey] = outList
#     if len(outList)>0:
#         if lineKey in matchDict.keys():
#             prevVal = matchDict[lineKey]
#             prevVal[lineDictKey] = outList
#             matchDict[lineKey]= prevVal
#         else:
#             newDict = {}
#             newDict[lineDictKey] = outList
#             matchDict[lineKey] = newDict
    
def readNamesFromNerMatcherOutput():
    f = open(inputPathString,"r")
    
    keyList = ['singleMatchedEntities_PER', 'singleMatchedEntities_ORG', 'crossmatchedEntities_PER', 'crossmatchedEntities_ORG']
    linesSum = 0
    for line in f:
        lineDict = parseData(line)
        explodeValues(lineDict,'singleMatchedEntities_PER',singleMatchDict_PER)
        explodeValues(lineDict,'singleMatchedEntities_ORG',singleMatchDict_ORG)
        explodeValues(lineDict,'crossmatchedEntities_PER',crossMatchDict_PER)
        explodeValues(lineDict,'crossmatchedEntities_ORG',crossMatchDict_ORG)
        linesSum += countValues(lineDict, keyList)
    #print "Matched name count = " + str(linesSum)
    return linesSum
    
def countValues(lineDict,keyList):
    lenSum = 0
    for key in keyList:
        oneLen = len(lineDict[key])
        lenSum += oneLen
    return lenSum

def extracRegcodes(matchedEntityList,lineRegcodeSet,extractBirthdate=False):
    for matchedEntity in matchedEntityList:
        #print matchedEntity
        regcode = matchedEntity[1][0]
        if extractBirthdate == True:
            regcode = getBirthDate(regcode)
        lineRegcodeSet.add(regcode)
    return lineRegcodeSet

def readregcodesFromNerMatcherOutput(uriWithRegcodesDict):
    f = open(inputPathString,"r")
    for line in f:
        lineDict = parseData(line)
        lineRegcodeSet = set()
        lineRegcodeSet = extracRegcodes(lineDict['singleMatchedEntities_PER'],lineRegcodeSet,extractBirthdate=True)
        lineRegcodeSet = extracRegcodes(lineDict['singleMatchedEntities_ORG'],lineRegcodeSet,extractBirthdate=False)
        lineRegcodeSet = extracRegcodes(lineDict['crossmatchedEntities_PER'],lineRegcodeSet,extractBirthdate=True)
        lineRegcodeSet = extracRegcodes(lineDict['crossmatchedEntities_ORG'],lineRegcodeSet,extractBirthdate=False)
        lineKey = lineDict["hostname"] + "::" + lineDict["path"] + "::" + lineDict["date"]
        uriWithRegcodesDict[lineKey] = lineRegcodeSet

    return uriWithRegcodesDict

def readNamesFromBenchmarkCsv(csvDict_PER,csvDict_ORG,csvDict_other):
    reader = csv.reader(open(csvPathString))
    for line in reader:
        lineKey = line[0] + "::" + line[1] + "::" +line[2]
        tag = line[4]
        tag2 = line[5]
        cleanName = line[6]
        regcode = line[8]
        if len(cleanName) > 0:
            namePart = cleanName
        else:
            namePart = line[3]
        namePart = namePart.decode("raw_unicode_escape","replace")
        value = csvRow()
        value.tag = tag
        value.tag2 = tag2
        value.cleanName = namePart
        value.regcode = regcode
        if tag == "o":
            insertIntoCsvDict(csvDict_ORG, lineKey, value)
        elif tag == "p":
            insertIntoCsvDict(csvDict_PER, lineKey, value)
        else:
            insertIntoCsvDict(csvDict_other, lineKey, value)
            
def insertIntoCsvDict(csvDict, key, value):
    if key in csvDict.keys():
        oldVal = csvDict[key]
        oldVal.append(value)
        csvDict[key] = oldVal
    else:
        csvDict[key] = [value]

def readRegcodesFromBenchmarkCsv():
    reader = csv.reader(open(csvPathString))
    regcodeDict = {}
    for line in reader:
        tag = line[4]
        tag2 = line[5]
        regcode = line[8]

        lineKey = line[0] + "::" + line[1] + "::" +line[2]
        oneUriRegcodeSet = set()
        try:
            oneUriRegcodeSet = regcodeDict[lineKey]
        except:
            pass
        
        if tag != "o" and tag != "p":
            pass
        elif len(regcode) < 4 or regcode == "None":
            pass
        else:
            oneUriRegcodeSet.add(regcode)
        regcodeDict[lineKey] = oneUriRegcodeSet
    return regcodeDict
            
def extractUsableCsvRows(csvDict):
    """
    Removes rows that are not ORG or PER or which lack regcode
    """
    newDict = {}
    for key in csvDict.keys():
        rowValsList = csvDict[key]
        usableList = []
        otherList = []
        for rowVal in rowValsList:
            if rowVal.tag != 'o' and rowVal.tag != 'p':
                otherList.append(rowVal)
                continue
            if len(rowVal.regcode)<4 or rowVal.regcode == "None":
                otherList.append(rowVal)
                continue
            if rowVal.tag2 == "faulty":
                otherList.append(rowVal)
                continue                
            usableList.append(rowVal)
        newDict[key] = usableList  
    return newDict

def getBirthDate(idStr):
    if len(idStr) is 13:
        #possibly foreign person with "1KYYYYMMDD***" id
        if idStr.startswith("1K"):
            yearStr = idStr[2:6]
            monthStr = idStr[6:8]
            dayStr = idStr[8:10]
            return dayStr+"."+monthStr+"."+yearStr
    if len(idStr) is 11:
        #possibly Estonian personal ID with "*YYMMDD*****" id
        century = ((int(idStr[0])-1) // 2) + 18
        yearStr = str(century*100 + int(idStr[1])*10 +  int(idStr[2]))
        monthStr = str(idStr[3] + idStr[4])
        dayStr = str(idStr[5] + idStr[6])
        return dayStr+"."+monthStr+"."+yearStr
    else:
       # print("Not an Estonian PER number: " + str(idStr))
        return idStr
        
def countDictRegcodes(uriWithRegcodesDict):
    count = 0
    for regcodeList in uriWithRegcodesDict.values():
        for regCode in regcodeList:
            count = count + 1
    return count

def matchByKey(nameDict,csvDict,convertId=False):
    """
    Outputs same-key data from name dict and csv dict
    """
    trueposList = []
    falseposList = []
    for key in nameDict.keys():
        if key in csvDict.keys():
            csvVals = csvDict[key]
            nameVals = nameDict[key]
            for oneVal in nameVals:
                matchFound = False
                regcode = oneVal[0]
                for oneCsvVal in csvVals:
                    csvRegcode = oneCsvVal.regcode
                    #print "regcode " + regcode + " -- " + csvRegcode
                    if convertId:
                        #print regcode
                        regcode = getBirthDate(regcode)
                    if csvRegcode == regcode:
                        #print "match " + unicode(oneVal)
                        trueposList.append((oneVal,oneCsvVal))
                        matchFound = True
                        break
                if not matchFound:
                    falseposList.append((key,oneVal))
        else:
            #print "No csvDict key " + str(key)
            nameVals = nameDict[key]
            for oneVal in nameVals:
                falseposList.append((key,oneVal))
    print "trueposlist: " + str(len(trueposList))
#     print "falseposlist: " + str(len(falseposList))
    return trueposList, falseposList

def findFalseNegatives(csvDict,matchList):
    #print matchList
    matchRows = []
    for match in matchList:
        matchRows.append(match[1])
    falsenegList = []
    for key in csvDict.keys():
        csvVals = csvDict[key]
        for oneCsvVal in csvVals:
            if oneCsvVal not in matchRows:
                falsenegList.append(oneCsvVal)
                #print oneCsvVal.regcode
    return falsenegList

def matchBySet(uriWithRegcodesDict,csvUriWithRegcodesDict):
    truepos = 0
    for urikey in uriWithRegcodesDict.keys():
        uriSet = uriWithRegcodesDict[urikey]
        try:
            csvSet = csvUriWithRegcodesDict[urikey]
            truepos = truepos + len(uriSet.intersection(csvSet))
        except KeyError:
            print "Keyerror: " + urikey
    return truepos

def calculateWithNames():
    global singleMatchDict_PER
    global singleMatchDict_ORG
    global crossMatchDict_PER 
    global crossMatchDict_ORG 
    global unmatchedDict
    singleMatchDict_PER = {}
    singleMatchDict_ORG = {}
    crossMatchDict_PER = {}
    crossMatchDict_ORG = {}
    unmatchedDict = {}
    n_retrieved = readNamesFromNerMatcherOutput()
#     print "single match PER uris: " + str(len(singleMatchDict_PER))
#     print "single match ORG uris: " + str(len(singleMatchDict_ORG))
#     print "cross match PER uris: " + str(len(crossMatchDict_PER))
#     print "cross match ORG uris: " + str(len(crossMatchDict_ORG))
    print "Retrieved = " + str(n_retrieved)
    
    csvDict_PER = {}
    csvDict_ORG = {}
    global csvDict_other
    csvDict_other = {}
    readNamesFromBenchmarkCsv(csvDict_PER,csvDict_ORG,csvDict_other)
    csvDict_PER = extractUsableCsvRows(csvDict_PER)
    csvDict_ORG = extractUsableCsvRows(csvDict_ORG)
#     print "csv PER uris: " + str(len(csvDict_PER))
#     print "csv ORG uris: " + str(len(csvDict_ORG))
    n_relevant = countDictRegcodes(csvDict_PER) + countDictRegcodes(csvDict_ORG)
    print "Relevant = " + str(n_relevant)

    #print csvDict_ORG.keys()
    print "-- single match PER --"
    truepos_single_PER, falsepos_single_PER = matchByKey(singleMatchDict_PER,csvDict_PER,convertId=True)
    print "-- single match ORG --"
    truepos_single_ORG, falsepos_single_ORG = matchByKey(singleMatchDict_ORG,csvDict_ORG)
    print "-- cross match PER --"
    truepos_cross_PER, falsepos_cross_PER = matchByKey(crossMatchDict_PER,csvDict_PER,convertId=True)
    print "-- cross match ORG --"
    truepos_cross_ORG, falsepos_cross_ORG = matchByKey(crossMatchDict_ORG,csvDict_ORG)
    
    truepos = truepos_single_PER + truepos_single_ORG + truepos_cross_PER + truepos_cross_ORG
    falsepos = falsepos_single_PER + falsepos_single_ORG + falsepos_cross_PER + falsepos_cross_ORG
    n_truepos = len(truepos)
    print "n_truepos = " + str(n_truepos) # matches were found and expected
    print "n_falsepos = " + str(len(falsepos)) # matches were found, but not expected to exist
    
    falseneg = findFalseNegatives(csvDict_PER, truepos) + findFalseNegatives(csvDict_ORG, truepos) # matches were not found, but were expected to exist
    print "n_falseneg = " + str(len(falseneg))
    precision = n_truepos/float(n_retrieved)
    recall = n_truepos/float(n_relevant)
    fscore = (2*precision*recall)/(precision + recall)
    print "Precision : " + str(precision)
    print "Recall : " + str(recall)
    print "F-score : " + str(fscore)
    print "\n"
    
#    for oneneg in falseneg:
#        print oneneg.regcode + " " + oneneg.cleanName
    for oneFalsePos in falsepos:
        print oneFalsePos

def calculateWithSets():
    uriWithRegcodesDict = {}
    uriWithRegcodesDict = readregcodesFromNerMatcherOutput(uriWithRegcodesDict)
#    print uriWithRegcodesDict
    n_retrieved = countDictRegcodes(uriWithRegcodesDict) 
    print "Retrieved : " + str(n_retrieved)

    csvUriWithRegcodesDict = {}
    csvUriWithRegcodesDict = readRegcodesFromBenchmarkCsv()
    n_relevant = countDictRegcodes(csvUriWithRegcodesDict)
    print "Relevant: " + str(n_relevant)

    n_truepos = matchBySet(uriWithRegcodesDict,csvUriWithRegcodesDict)
    precision = n_truepos/float(n_retrieved)
    recall = n_truepos/float(n_relevant)
    fscore = (2*precision*recall)/(precision + recall)
    print "--Same-URI set of regcodes based results--"
    print "Precision \t " + str(precision)
    print "Recall   \t " + str(recall)
    print "F-score \t " + str(fscore)
    
def main():
    calculateWithNames()
#    calculateWithSets()
    
"""
'singleMatchedEntities_PER', 
'singleMatchedEntities_ORG', 
'crossmatchedEntities_PER', 
'crossmatchedEntities_ORG', 
'nonCrossmatchedNames_PER', 
'nonCrossmatchedNames_ORG', 
'notMatchableNames_PER'
'notMatchableNames_ORG', 
'matchlessNames', 
'hostname', 
'path', 
'date', 
'distinctNames', 
"""        

if __name__=="__main__":
    inputPathString = "00and02_x171/00and02_newcsv_x171"
    csvPathString = "csv/names_with_codes_17oct16_v1.csv"
    main()