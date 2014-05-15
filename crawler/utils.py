#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

keys_merchandising = ['FIGURAS', 'FIGURA', 'FIG', 'ESTATUAS', 'ESTATUA',
 'CAMISETA', 'TAZA', 'LLAVERO', 'BUSTO', 'CABEZON', 'PELUCHES', 'PELUCHE',
 u'MUÑECOS', u'MUÑECO', 'IMANES', 'IMAN']

def get_title_collection(title, category, manufacturer):
	""" extract the title collection from the title if exists """
	
	#remove parentheses
	title = re.sub(r'\([^)]*\)', '', title).strip()
	
	
	if "MERCHANDISING" in category:
		for k in keys_merchandising:
			try:
				if k in title.decode("utf-8"):
					return "%s %s" % (k if k != "FIG" else "FIGURA", manufacturer)
			except UnicodeEncodeError:
				if k in title:
					return "%s %s" % (k if k != "FIG" else "FIGURA", manufacturer)
				
		return "VARIOS %s" % (manufacturer)
	else:
		#~ print title
		if " VOL " in title or " VOL." in title:
			return title.split(" VOL")[0]
		if " vol " in title or " vol." in title:
			return title.split(" vol")[0]
		
		#~ print "\t\t-"
		#try with separator chars
		exceptions = ["-x"]
		
		
		first_word = re.findall(r"[\w']+", title)[0]

		#exclude first word
		domain = title if any(v in first_word.lower() for v in u"aeiouáéíóú") else title.replace(first_word, "", 1)
		  
		#~ print title.split()[0].lower()
		#~ print "\t", title, ":::", domain
		  
		if not any(e in domain for e in exceptions):
			for sep in ".:-":
				if sep in domain[1:]:
					title_collection = title.split(sep)[0].strip()
					if title != title_collection:
						break
		
		try:
			if title_collection != title:
				title = title_collection
		except UnboundLocalError:
			title_collection = title
		
		
		#search numbers 1, 10, 03, 002, 462 ..
		title_prenumber = title_collection
		try:
			title_collection = re.findall("(.*?) [0-9]{1}[^0-9]?.*?",title)[0]
		except IndexError:
			try:
				title_collection = re.findall("(.*?) [0-9]{2}[^0-9]?.*?",title)[0]
			except IndexError:
				try:
					title_collection = re.findall("(.*?) [0-9]{3}[^0-9]?.*?",title)[0]
				except IndexError:
					pass
					
		try:
			last_word = re.findall(r"[\w']+", title_collection)[-1]
			#gramtical exceptions
			number_exceptions = ["el", "la", "los", "las", "un", "una", "unos", "unas"]
			if last_word.lower() in number_exceptions:
				title_collection = title_prenumber
		except IndexError:
			pass
			
	try:
		while "  " in title_collection:
			title_collection = title_collection.replace("  ", " ")
	except UnboundLocalError:
		title_collection = title
	
	try:
		if title_collection[-1] in ":.,-":
			title_collection = title_collection[:-1]
	except IndexError:
		pass
		
	removed = [u"Edición integral"]
	for r in removed:
		try:
			repl = r in title_collection
		except UnicodeDecodeError:
			repl = r in title_collection.decode("utf-8")
				
		if repl:
			try:
				title_collection = title_collection.replace(r, "").strip()
			except UnicodeDecodeError:
				title_collection = title_collection.decode("utf-8").replace(r, "").strip()
				
	
	return title_collection
	
def get_number_collection(title, _id, category):
	""" extract the number collection from the title if exists """
	
	#remove parentheses
	title = re.sub(r'\([^)]*\)', '', title)
	
	number_collection = None
	if not "MERCHANDISING" in category:
		#merchandising is special case
		try:
			number_collection = int(re.findall(".*? ([0-9]{1})[^0-9]?.*?",title)[0])
		except IndexError:
			try:
				number_collection = int(re.findall(".*? ([0-9]{2})[^0-9]?.*?",title)[0])
			except IndexError:
				try:
					number_collection = int(re.findall(".*? ([0-9]{3})[^0-9]?.*?",title)[0])
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

def test_get_title_collection(verbose = False):
	tests = {
		"ASOMBROSO SPIDERMAN 068" : [False, "ASOMBROSO SPIDERMAN"],
		"ASTERIX 01: EL GALO" : [False, "ASTERIX"],
		"FUNDAS ILUSTRADAS - EYE OF SAURON (50) UNLIMITED EDITION - SDLA" : [False, "FUNDAS ILUSTRADAS"],
		"EL PODEROSO THOR: EL FIN DE  LOS ETERNOS  (MARVEL (MARVEL GOLD)" : [False, "EL PODEROSO THOR"], 
		"KING OF THORN. EL REY ESPINO -BLU RAY" : [False, "KING OF THORN"],  
		"X-WING: TRANSPORTE REBELDE" : [False, "X-WING"],  
		"MEMORIAS DE IDHUN. PANTEON (5) CONVULSION" : [False, "MEMORIAS DE IDHUN"],  
		"ASQUEROSAMENTE RICA  (PANINI NOIR)" : [False, "ASQUEROSAMENTE RICA"],  
		"MAGOS HUMOR 116. EUROBASKET 2007" : [False, "MAGOS HUMOR"],  
		"X-FACTOR FOREVER. FAMILIA" : [False, "X-FACTOR FOREVER"],  
		"SOLO. LOS SUPERVIVIENTES DEL CAOS 01" : [False, "SOLO"],  
		"LA BIPOLARIDAD DEL CHOCOLATE 1. EL VIAJE DE JAN" : [False, "LA BIPOLARIDAD DEL CHOCOLATE"],  
		"BALEARIC. PREMIO ART JOVE 2010" : [False, "BALEARIC"],  
		"LOS REYES ELFOS: HISTORIAS DE FAERIE 02" : [False, "LOS REYES ELFOS"],  
		"MARVEL MASTERWORKS 02: LOS 4 FANTASTICOS (1963)" : [False, "MARVEL MASTERWORKS"],  
		"SI, DIBUJO TEBEOS ¿Y QUE?" : [False, "SI, DIBUJO TEBEOS ¿Y QUE?"],  
		"BY VAZQUEZ. 80 AÑOS DEL NACIMIENTO DE UN MITO" : [False, "BY VAZQUEZ"],  
		"MESTRES DE L´HUMOR 31. LONDRES 2012" : [False, "MESTRES DE L´HUMOR"],  
		"SUPER CALVIN Y HOBBES 10: PAGINAS DOMINICALES 1985-1995" : [False, "SUPER CALVIN Y HOBBES"],  
		"D.GRAY MAN 06 (COMIC)" : [False, "D.GRAY MAN"],  
		"LOS VENGADORES VOL 4 40" : [False, "LOS VENGADORES"],  
		"LOS 4 FANTASTICOS" : [False, "LOS 4 FANTASTICOS"],  
		"LOS 4 FANTASTICOS VOL. 7 077" : [False, "LOS 4 FANTASTICOS"],  
		"DR. INUGAMI (MARUO) (EDICION CARTONE)" : [False, "DR. INUGAMI"],  
		"MALVIVIENDO: EL TEBEO VOL 3" : [False, "MALVIVIENDO: EL TEBEO"],  
		"ASGARD Edición integral" : [False, "ASGARD"],  
		"BLAKE Y MORTIMER 09. EL SECRETO DEL ESPADÓN (1ª PARTE)  PERSECUCIÓN FANTÁSTICA" : [False, "BLAKE Y MORTIMER"],  
		"Mc FARLANE/MAGGIE FIGURA" : [True, "FIGURA Mc FARLANE"],  
		"NECA/GOLLUM FIGURA 5 CM SCALERS SERIE 1" : [True, "FIGURA NECA"],  
		"NECA/GOLLUM FIG 5 CM SCALERS SERIE 1" : [True, "FIGURA NECA"],  
		"SD TOYS/AS HIGH AS HONOR ARRYN CAMISETA CHICA T" : [True, "CAMISETA SD TOYS"],  
		"SD TOYS/CORPSE BRIDE SET A PUNTO DE LIBRO MAGNETICO LA NOVIA CADAVER" : [True, "VARIOS SD TOYS"],  
		"FUNKO/BIKER SCOUT FIG.10 CM VINYL POP STAR WARS" : [True, "FIGURA FUNKO"],  
		"NECA/JAEGER ESENCIALES SURTIDO 2 FIGURAS" : [True, "FIGURAS NECA"]
		}
		
	for title, result in tests.items():
		if result[0]:
			#merchandising
			a_title = title.split("/")
			extract = get_title_collection(a_title[1], "MERCHANDISING", a_title[0])
		else:
			extract = get_title_collection(title, "a", "b")
		
		if extract != result[1]:
			if verbose:
				print "No ha pasado la prueba %s => %s (%s)" % (title, result[1], extract)
			else:
				return False
	
	return True

#test get_title_collection
if __name__ == '__main__':
	print test_get_title_collection(verbose = True)
	


