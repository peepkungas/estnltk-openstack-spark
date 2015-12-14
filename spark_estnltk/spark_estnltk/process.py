# -*- coding: cp1252 -*-
from pyspark import SparkContext, SparkConf
import sys
import justext
import estnltk
import logging


def parseHtmlToText(htmlContent):
    try:
        justextContent = justext.justext(htmlContent.encode("utf-8"), justext.get_stoplist('Estonian'))
#         text = getText(getParagraphs(justextContent))
    except Exception:
        justextContent = ""
    text = getText(getParagraphs(justextContent))
    #logger.info("Text length:"+len(text))
    return text

def getParagraphs(paragraphs):
    out = []
    for paragraph in paragraphs:
        if (paragraph.is_boilerplate == False):
            out.append(paragraph.text)
    return out

def getText(goodparagraphs):
    if len(goodparagraphs) == 0:
        return None
    allparagraphs = ""
    for paragraph in goodparagraphs:
        allparagraphs += paragraph + "\r\n"
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

def writeToFile(filepath, key, values):
    '''
    outputPairRDD = spark.parallelize((key,values))
    outputPairRDD.saveAsTextFile(filepath)
    '''
    '''
    #TODO : HDFS
    if (filepath.startswith("hdfs://")):
        pass
    
    # local filesystem
    else:
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))
            with open(filepath, "w") as f:
                    f.write(values)
    '''
    
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

def processSequencePair(key, value):
    returndict = {}
    
    
#     try:
    if (isPlaintextInput == True):
        text = estnltk.Text(value)
    else:
        text = parseHtmlToText(value)
#     except Exception:
# #         print ("ERR: Failed to parse : " + key)
#         return (key, "FAILED_TO_PARSE: " + str(Exception))
#     
    if text == None:
        return (key, "NO_TEXT")
    
    try:
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
                print ("WARN: Failed to lemmatize : " + key)
        
        if (doPostagging == True):
            try:
                text.postags
            except Exception:
                print ("WARN: Failed to find postags : " + key)
        
        if (doMorphological == True):
            try:
                text.analysis
            except Exception:
                print ("WARN: Failed to perform morphological analysis : " + key)
        
        if (doNer == True):
            try:
                text.named_entities
            except Exception:
                print ("WARN: Failed to perform Named Entity Recognition : " + key)
    except Exception:
        print("ERR: Exception when estnltk processing " + key)
        return (key, "ERR_ESTNLTK")
    # TODO : write to file (key as dir)
    '''
    dir1, dir2, dir3 = key.split(splitterStr)
    outpath = sys.argv[2] + "/" + dir1 + "/" + dir2 + "/" + dir3
    '''
    try:
        if (outTokenization == True):
            if (tokenLevel == "word"):
                values = text.get.words.as_dict
            if (tokenLevel == "sentence"):
                values = text.get.sentences.as_dict
            if (tokenLevel == "paragraph"):
                values = text.get.paragraphs.as_dict
            returndict["token"] = values
            '''
            filepath = outpath + "/token"   
            writeToFile(filepath, key, values)
            '''
        if (outLemmatization == True):
            values = text.get.lemmas.as_dict
            returndict["lemma"] = values
            '''  
            filepath = outpath + "/lemma"
            writeToFile(filepath, key, values)
            '''     
        if (outPostagging == True):
            values = text.get.postags.as_dict
            returndict["postag"] = values  
            '''
            filepath = outpath + "/postag"
            writeToFile(filepath, key, values)
            '''
            
        if (outMorphological == True):
            values = text.get.analysis.as_dict
            returndict["morph"] = values
            '''
            filepath = outpath + "/morph"
            writeToFile(filepath, key, values)
            '''
        if (outNer == True):
            values = text.get.named_entities.named_entity_labels.as_dict
            returndict["ner"] = values
            '''
            filepath = outpath + "/ner"
            writeToFile(filepath, key, values)
            '''
        if (outAll == True):
            values = text.get.words.lemmas.postags.analysis.named_entities.as_dict
            returndict["all"] = values 
            '''
            filepath = outpath + "/all"
            writeToFile(filepath, key, values)
            ''' 
    except Exception:
        print("ERR: Exception in output/return: " + key )
        return (key,"ERR_OUTPUT")
    ## TODO ? : return result instead if text object ? (key:wantedvalues pair)
    
    return (key, returndict)








if __name__ == "__main__":
    # Get wanted estnltk processes from system args

    splitterStr = "::"
    processarguments = sys.argv[3:]

    tokenLevel = "paragraph"
    if ("-tokenLevel=word" in processarguments):
        tokenLevel = "word"
    elif ("-tokenLevel=sentence" in processarguments):
        tokenLevel = "sentence"
    elif ("-tokenLevel=paragraph" in processarguments):
        tokenLevel = "paragraph"

    # If true, treat input as plaintext (no HTML cleaning is to be done)
    isPlaintextInput = False
    if ("-isPlaintextInput" in processarguments):
        isPlaintextInput = True
        
    # If true, include boilerplate paragraphs (more info included in results, but much more noise)
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
        
        outAll = True
    else:
        doAll = False
        outAll = False
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
    
# #    startTime = time.clock()
            
    # Initialize Spark
    conf = SparkConf()
    spark = SparkContext(appName="estnltk_seqfile_analyser", conf=conf)

    logger = logging.getLogger('pyspark') # logging does not work, pickling err
    # Open input files
    input_files = spark.sequenceFile(sys.argv[1])    
    

    # Perform all processes in one map
    keytextrdd = input_files.map(lambda keyval : processSequencePair(keyval[0], keyval[1]))
    keytextrdd.coalesce(1).saveAsTextFile(sys.argv[2])
    '''

    # Clean the input files (html -> only text content). Justext returns paragraphs.
    if (isPlaintextInput == True):
        keytextpairs = input_files.map(lambda line : (line[0], estnltk.Text(line[1])))
    else:
        keytextpairs = input_files.map(lambda line : (line[0], parseHtmlToText(line[1])))

# #    emptytextpairs = keytextpairs.filter(lambda keytext : len(keytext[1].words)==0)
    keytextpairs = keytextpairs.filter(lambda keytext : len(keytext[1].words) > 0)

    # Apply estnltk processes.
    # As relevant info is saved into Text objects when first called, we can create them here and access them later.
    keyprocessedtextpairs = keytextpairs.mapValues(lambda text: apply_estnltk_processes(text))
    
# #    debugpairs = keyprocessedtextpairs.mapValues(lambda value : len(value.words))
# #    debugpairs.saveAsTextFile(sys.argv[2]+"/debug")
    if (doAll == True):
        keyallpairs = keyprocessedtextpairs.mapValues(lambda text : callGetAll(text))
        keyallpairs.saveAsTextFile(sys.argv[2] + "/all")
    
    # Word, sentence, paragraph tokenization (http://tpetmanson.github.io/estnltk/tutorials/tokenization.html)
    if (outTokenization == True):
        if (tokenLevel == "word"):
            keytokenpairs = keyprocessedtextpairs.mapValues(lambda text : callGetWords(text))
        elif (tokenLevel == "sentence"):
            keytokenpairs = keyprocessedtextpairs.mapValues(lambda text : callGetSentences(text))
        elif (tokenLevel == "paragraph"):
            keytokenpairs = keyprocessedtextpairs.mapValues(lambda text : callGetParagraphs(text))
        else:
            keytokenpairs = keyprocessedtextpairs.mapValues(lambda text : callGetParagraphs(text))
        keytokenpairs.saveAsTextFile(sys.argv[2] + "/token")

    # Lemmatization (http://tpetmanson.github.io/estnltk/tutorials/morf_analysis.html)
    if (outLemmatization == True):
        keylemmapairs = keyprocessedtextpairs.mapValues(lambda text : callGetLemmas(text))
        keylemmapairs.saveAsTextFile(sys.argv[2] + "/lemma")

    # POS tagging (http://tpetmanson.github.io/estnltk/tutorials/morf_analysis.html)
    if (outPostagging == True):
        keypostagpairs = keyprocessedtextpairs.mapValues(lambda text : callGetPostags(text))
        keypostagpairs.saveAsTextFile(sys.argv[2] + "/postag")
        
    # Morphological analysis (http://tpetmanson.github.io/estnltk/tutorials/morf_analysis.html)
    if (outMorphological == True):
        keymorphpairs = keyprocessedtextpairs.mapValues(lambda text : callGetAnalysis(text))
        keymorphpairs.saveAsTextFile(sys.argv[2] + "/morph")
        
    # Named entity recognition (http://tpetmanson.github.io/estnltk/tutorials/ner.html)
    if (outNer == True):
        keynerpairs = keyprocessedtextpairs.mapValues(lambda text : callGetNamedEntities(text))
        keynerpairs.saveAsTextFile(sys.argv[2] + "/ner")

    '''
# #    spark.stop()
# #    endTime = time.clock()
# #    print("Time spent:" + str(endTime-startTime))
