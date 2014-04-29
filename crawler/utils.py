#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re


def get_title_collection(title):
	""" extract the title collection from the title if exists """
	
	try:
		title_collection = re.findall("(.*) [0-9]{3}.*",title)[0]
	except IndexError:
		try:
			title_collection = re.findall("(.*) [0-9]{2}.*",title)[0]
		except IndexError:
			title_collection = title
			
	if title_collection == title:
		#try with separator chars
		exceptions = ["-x"]
		
		if not any(e in title.lower() for e in exceptions):
			for sep in ",-:":
				title_collection = title.split(sep)[0].strip()
				if title != title_collection:
					break
	
	return title_collection
	
def get_number_collection(title):
	""" extract the number collection from the title if exists """
	
	try:
		number_collection = int(re.findall(".* ([0-9]{3}).*",title)[0])
	except IndexError:
		try:
			number_collection = int(re.findall(".* ([0-9]{2}).*",title)[0])
		except IndexError:
			number_collection = None
	
	return number_collection
  
