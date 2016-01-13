from pyspark import SparkContext, SparkConf
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


def parseData(line):
        x,y = ast.literal_eval(line)

        # x - hostname::path::date
        # y - dictionary
        # y['named_entity_labels'] - list of labels
        # y['named_entities'] - list if entity strings
        # when y - dictionary, is empty it contains "NO_TEXT", this entries are filtered out 
        if y == "NO_TEXT":
            return None
        return x, y

def  explodeEntries(line):
        key,value = line
        values = []
        # look through the two lists at the same time and append each of them into a list together with the hostname::path::date string
        for entry,label in zip(value['ner']['named_entities'],value['ner']['named_entity_labels']):
            values.append(((entry, label), key))
        return values

def parseEntities(line):
        # split values by comma which represents separator and assign them into a tuple.
        values = line.split("\",\"")
        # discard lines which contain less than nine columns
        if(len(values)!=9):
            return None
        return tuple(values)


def  filterEntities(line):
        grouped,entities = line
        entlab,keys = grouped
        entity,label = entlab
        # did not label all the fields here
		# even though the invalid lines are filtered out on the previous stage. Just a safety net. 
        try:
           b0,b1,b2,b3,b4,b5,b6,b7,b8 = entities
        except ValueError:
           return False

        # some issues with strings containing \" or \' characters
        b1 = b1.replace("\"", "")
        b1 = b1.replace("\'", "")
        b2 = b2.replace("\"", "")
        b2 = b2.replace("\'", "")


        #print aaa.encode('utf-8').strip(), "-" ,b1.encode('utf-8').strip(), "-",b2.encode('utf-8').strip(), "\n\n"

        # currently just checking whether first and last names or the company name is contained in the entity string
        # Works with Eduard Viira
        #
        # Not sure how EXACTLY would it becorrect to match the entity descriptions but this is not the goal of this example
        # TODO: Currently not cheking the registration date!

        # if first and last names are contained in the entity string.
        # TODO: order?
        if(b1 in entity and b2 in entity):
                return True

        # if the company name is contained in the entity string.
        #TODO: what if the match is not exact? order of words? "General motors" vs "General mostors LIMITED", vs "GM LIMITED", etc.
		#observed from the input files, but needs specification:
		#the company identifier string contains vector of company names "General motors|General motors LIMITED". Therefore this approach should be suffcient
        if(b5 in entity):
                return True

        #otherwise filter the entry out
        return False

def cleanResult(line):
        results = []

        grouped,entities = line
        entlab,keys = grouped
        entity,label = entlab

        # did not label all the fields here
        b0,b1,b2,b3,b4,b5,b6,b7,b8 = entities

        for key in keys:
                results.append((key, entity, b0,b1,b2,b3))
        return results

def entitiesToSearchIn(line):
        key,value = line
        return key[1]=="PER" or key[1]=="ORG"

if __name__ == "__main__":
	
    if len(sys.argv) != 3:
        print len(sys.argv)
        print "estlinkSpark.py <inputPath> <csvFilePath>"
        sys.exit()

    startTime = time.clock()

    inputPath = sys.argv[1]
    csvFilePath = sys.argv[2]

    conf = SparkConf()

    spark = SparkContext(appName="NER_Demo", conf=conf)

    #load list of known entities
    entities = spark.textFile(csvFilePath)

    #load named entity recognition data
    data = spark.textFile(inputPath)

    # list of key-value pairs where:
    # key: hostname::path::date
    # value: dictionary with ['named_entity_labels'] and ['named_entities'] entries
    #filter out invalid entries
    entries = data.map(lambda line: parseData(line)).filter(lambda entry: entry is not None)
    #explode each (key, [named_entity_labelsvalues],  [named_entities]) entry into:
    # [(named_entity, named_entity_label), key] list
    # In essence making a reverse index
    exploded = entries.flatMap(lambda line: explodeEntries(line))
    # group values together by (named_entity, named_entity_label) as key
    grouped = exploded.groupByKey()
    #join known entities with each of the extracted entity groups
    #filter out invalid entities
    cleaned_entities = entities.map(lambda line: parseEntities(line)).filter(lambda entry: entry is not None)
    grouped = grouped.filter(lambda line: entitiesToSearchIn(line))
    # make a cartesian product
    # Most likely too unefficient, and a better solution needs to be found
    joined = grouped.cartesian(cleaned_entities)
    #filter out rows that do not contain same entities
    filtered = joined.filter(lambda line: filterEntities(line))
    # Clean the results by exploding the "hostname::path::datetime" keys and extracting only some of the values that interest us
    cleanedResults = filtered.flatMap(lambda line: cleanResult(line)).distinct()
    #write results into a randomly numbered  folder under the "out" folder
    cleanedResults.saveAsTextFile("out/out_"+str(random.randint(0,10000)))

    spark.stop()
    endTime = time.clock()
    print "Time to finish tuned: 0.066288"
    print "Time to finish:" + str(endTime - startTime)
