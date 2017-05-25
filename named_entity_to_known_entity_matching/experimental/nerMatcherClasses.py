#!/usr/bin/env python
# -*- coding: utf-8 -*-

#class HashableDict(dict):
# ## DEPRECATED
#    def __key(self):
#        return tuple((k,self[k]) for k in sorted(self))
#    def __hash__(self):
#        return hash(self.__key())
#    def __eq__(self, other):
#        return self.__key() == other.__key()

class NamedEntity():
    name = u""   # Original name of entity in source document
    sourceId = u"" # Unique identifier of source document
    normalizedForms = []    # Normalized forms of original name
    ngramKeys = [] # Keys from combining n-grams of normalized forms
    matchingKnownEntityKeys = [] # Normalized names of closest matching known entity
    matchingKnownEntities = [] # (xxdata, [yyRelsdata]) pairs describing known entities matched to this named entity
    allowMatchingAsPER = True # Flag to allow/disallow matching to known persons
    def __str__(self):
        return "({0},{1})".format(self.name,[data[0] for data in self.matchingKnownEntities])
    def __unicode__(self):
        return u"({0},{1})".format(self.name,[data[0] for data in self.matchingKnownEntities])
#    def __eq__(self, other):
#        """
#        Override the default Equals behavior
#        """
#        if isinstance(other, self.__class__):
#            if not (self.name == other.name):
#                return False
#            if not (self.sourceId == other.sourceId):
#                return False
#	        return True
#	    return NotImplemented
#    def __hash__(self):
#        return hash(self.name+"::"+self.sourceId)    	    	 