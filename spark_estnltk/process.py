# -*- coding: cp1252 -*-
from pyspark import SparkContext, SparkConf
import sys
from operator import add
import re
import justext
import urllib2
import estnltk
import time

def getParagraphs(paragraphs, doIncludeBoilerplate):
    out = []
    for paragraph in paragraphs:
      if doIncludeBoilerplate or not paragraph.is_boilerplate:
        out.append(paragraph.text)
    return out

def getText(goodparagraphs):
    allparagraphs = ""
    for paragraph in goodparagraphs:
        allparagraphs += paragraph +"\n"
    text = estnltk.Text(allparagraphs)
    return text

def callGetWords(text):
    try:
        return text.get.words.as_dict
    except Exception:
        return None

def callGetSentences(text):
    try:
        return text.get.sentences.as_dict
    except Exception:
        return None

def callGetParagraphs(text):
    try:
        return text.get.paragraphs.as_dict
    except Exception:
        return None

def callGetLemmas(text):
    try:
        return text.get.lemmas.as_dict
    except Exception:
        return None

def callGetPostags(text):
    try:
        return text.get.postags.as_dict
    except Exception:
        return None

def callGetAnalysis(text):
    try:
        return text.get.analysis.as_dict
    except Exception:
        return None

def callGetNamedEntities(text):
    try:
        return text.get.named_entities.named_entity_labels.as_dict
    except Exception:
        return None

def callGetAll(text):
    try:
        return text.get.words.lemmas.postags.analysis.named_entities.as_dict
    except Exception as e:
        return e

def apply_estnltk_processes(text):
    if (doTokenization == True):
        if (tokenLevel == "word"):
            text.tokenize_words
        elif (tokenLevel == "sentence"):
            text.tokenize_sentences
        elif (tokenLevel == "paragraph"):
            text.tokenize_paragraphs
        else:
            text.tokenize_paragraphs
    if (doLemmatization == True):
        try:
            text.lemmas
        except Exception:
            pass
    if (doPostagging == True):
        try:
            text.postags
        except Exception:
            pass
    if (doMorphological == True):
        try:
            text.analysis
        except Exception:
            pass
    if (doNer == True):
        try:
            text.named_entities
        except Exception:
            pass
    return text


if __name__ == "__main__":
    #Get wanted estnltk processes from system args

    processarguments = sys.argv[3:]

    tokenLevel = "paragraph"
    if ("-tokenLevel=word" in processarguments):
        tokenLevel = "word"
    elif ("-tokenLevel=sentence" in processarguments):
        tokenLevel = "sentence"
    elif ("-tokenLevel=paragraph" in processarguments):
        tokenLevel = "paragraph"

    doIncludeBoilerplate = False
    if ("-includeBoilerplate" in processarguments):
        doIncludeBoilerplate = True

    if ("-all" in processarguments):
        doAll = True
        doTokenization = True
        doLemmatization = True
        doPostagging = True
        doMorphological = True
        doNer = True
    else:
        doAll = False
    if ("-token" in processarguments):
        doTokenization = True
        outTokenization = True
    else:
        doTokenization = False
        outTokenization = False
    if ("-lemma" in processarguments):
        doLemmatization = True
        outLemmatization = True
    else:
        doLemmatization = False
        outLemmatization = False
    if ("-postag" in processarguments):
        doPostagging = True
        outPostagging = True
    else:
        doPostagging = False
        outPostagging = False
    if ("-morph" in processarguments):
        doMorphological = True
        outMorphological = True
    else:
        doMorphological = False
        outMorphological = False
    if ("-ner" in processarguments):
        doNer = True
        outNer = True
    else:
        doNer = False
        outNer = False
    
##    startTime = time.clock()
            
    #Initialize Spark
    conf = SparkConf()
    spark = SparkContext(appName="estlntk_seqfile_analyser", conf=conf)

    #Open input files
    input_files = spark.sequenceFile(sys.argv[1])    

    #Clean the input files (html -> only text content). Justext returns paragraphs.
    keytextpairs = input_files.map(lambda line : (line[0], getText(getParagraphs( justext.justext(line[1].encode("utf-8"), justext.get_stoplist('Estonian')), doIncludeBoilerplate))))
##    emptytextpairs = keytextpairs.filter(lambda keytext : len(keytext[1].words)==0)
    keytextpairs = keytextpairs.filter(lambda keytext : len(keytext[1].words)>0)

    #Apply estnltk processes.
    #As relevant info is saved into Text objects when first called, we can create them here and access them later.
    keyprocessedtextpairs = keytextpairs.mapValues(lambda text: apply_estnltk_processes(text))
##    debugpairs = keyprocessedtextpairs.mapValues(lambda value : len(value.words))
##    debugpairs.saveAsTextFile(sys.argv[2]+"/debug")
    if (doAll == True):
        keyallpairs = keyprocessedtextpairs.mapValues(lambda text : callGetAll(text))
        keyallpairs.saveAsTextFile(sys.argv[2]+"/all")
    
    #Word, sentence, paragraph tokenization (http://tpetmanson.github.io/estnltk/tutorials/tokenization.html)
    if (outTokenization == True):
        if (tokenLevel == "word"):
            keytokenpairs = keyprocessedtextpairs.mapValues(lambda text : callGetWords(text))
        elif (tokenLevel == "sentence"):
            keytokenpairs = keyprocessedtextpairs.mapValues(lambda text : callGetSentences(text))
        elif (tokenLevel == "paragraph"):
            keytokenpairs = keyprocessedtextpairs.mapValues(lambda text : callGetParagraphs(text))
        else:
            keytokenpairs = keyprocessedtextpairs.mapValues(lambda text : callGetParagraphs(text))
        keytokenpairs.saveAsTextFile(sys.argv[2]+"/token")

    #Lemmatization (http://tpetmanson.github.io/estnltk/tutorials/morf_analysis.html)
    if (outLemmatization == True):
        keylemmapairs = keyprocessedtextpairs.mapValues(lambda text : callGetLemmas(text))
        keylemmapairs.saveAsTextFile(sys.argv[2]+"/lemma")

    #POS tagging (http://tpetmanson.github.io/estnltk/tutorials/morf_analysis.html)
    if (outPostagging == True):
        keypostagpairs = keyprocessedtextpairs.mapValues(lambda text : callGetPostags(text))
        keypostagpairs.saveAsTextFile(sys.argv[2]+"/postag")
        
    #Morphological analysis (http://tpetmanson.github.io/estnltk/tutorials/morf_analysis.html)
    if (outMorphological == True):
        keymorphpairs = keyprocessedtextpairs.mapValues(lambda text : callGetAnalysis(text))
        keymorphpairs.saveAsTextFile(sys.argv[2]+"/morph")
        
    #Named entity recognition (http://tpetmanson.github.io/estnltk/tutorials/ner.html)
    if (outNer == True):
        keynerpairs = keyprocessedtextpairs.mapValues(lambda text : callGetNamedEntities(text))
        keynerpairs.saveAsTextFile(sys.argv[2]+"/ner")

##    spark.stop()
##    endTime = time.clock()
##    print("Time spent:" + str(endTime-startTime))
