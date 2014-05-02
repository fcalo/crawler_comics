#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

keys_merchandising = ['FIGURA', 'FIGURAS', 'FIG', 'ESTATUA', 'ESTATUAS',
 'CAMISETA', 'TAZA', 'LLAVERO', 'BUSTO', 'CABEZON', 'PELUCHE', 'PELUCHES',
 u'MUÑECO', u'MUÑECOS', 'IMAN', 'IMANES']

def get_title_collection(title, category, manufacturer):
	""" extract the title collection from the title if exists """
	
	#remove parentheses
	title = re.sub(r'\([^)]*\)', '', title).strip()
	
	
	if "MERCHANDISING" in category:
		for k in keys_merchandising:
			if k in title.decode("utf-8"):
				
				return "%s %s" % (k if k != "FIG" else "FIGURA", manufacturer)
		return "VARIOS %s" % (manufacturer)
	else:
		#try with separator chars
		exceptions = ["-x"]
	
		if not any(e in " ".join(title.split()).lower() for e in exceptions):
			for sep in ":.":
				title_collection = title.split(sep)[0].strip()
				if title != title_collection:
					break

			if title_collection == title:
				#exclude first word
				if not any(e in " ".join(title.split()[1:]).lower() for e in exceptions):
					for sep in ",-":
						if sep in " ".join(title.split()[1:]):
							title_collection = title.split(sep)[0].strip()
							if title != title_collection:
								break
		
		#search numbers 1, 10, 03, 002, 462 ..
		try:
			title_collection = re.findall("(.*?) [0-9]{1}[^0-9].*?",title)[0]
		except IndexError:
			try:
				title_collection = re.findall("(.*?) [0-9]{2}[^0-9].*?",title)[0]
			except IndexError:
				try:
					title_collection = re.findall("(.*?) [0-9]{3}[^0-9].*?",title)[0]
				except IndexError:
					pass
			
						
	while "  " in title_collection:
		title_collection = title_collection.replace("  ", " ")
		
	if title_collection[-1] in ":.,-":
		title_collection = title_collection[:-1]
	
	return title_collection
	
def get_number_collection(title, _id, category):
	""" extract the number collection from the title if exists """
	
	#remove parentheses
	title = re.sub(r'\([^)]*\)', '', title)
	
	number_collection = None
	if not "MERCHANDISING" in category:
		#merchandising is special case
		try:
			number_collection = int(re.findall(".*? ([0-9]{1})[^0-9].*?",title)[0])
		except IndexError:
			try:
				number_collection = int(re.findall(".*? ([0-9]{2})[^0-9].*?",title)[0])
			except IndexError:
				try:
					number_collection = int(re.findall(".*? ([0-9]{3})[^0-9].*?",title)[0])
				except IndexError:
					number_collection = None
			
	if not number_collection:
		number_collection = "".join(reversed([c for c in reversed(_id) if c.isdigit()]))
	
	return number_collection
  
def is_number(s):
    try:
        float(s)
        return True
    except:
        return False
