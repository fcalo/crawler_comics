#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import unicodedata

keys_merchandising = ['FIGURAS', 'FIGURA', 'FIG', 'ESTATUAS', 'ESTATUA',
 'CAMISETA', 'TAZA', 'LLAVERO', 'BUSTO', 'CABEZON', 'PELUCHES', 'PELUCHE',
 u'MUÑECOS', u'MUÑECO', 'IMANES', 'IMAN']
 
keys_merchandising_alias = {
    "FIG" : "FIGURAS",
    "FIGURA" : "FIGURAS",
    "IMAN" : "IMANES"
}

def get_title_collection(title, category, manufacturer, include_manufacturer = True, categories_like_merchandising = ['MERCHANDISING']):
    """ extract the title collection from the title if exists """
    
    #remove parentheses
    title = re.sub(r'\([^)]*\)', '', title).strip()
    
    if category in categories_like_merchandising:
        for k in keys_merchandising:
            try:
                if k in title.decode("utf-8"):
                    if include_manufacturer:
                        return "%s %s" % (k if k not in keys_merchandising_alias else keys_merchandising_alias[k], manufacturer)
                    else:
                        return "%s" % (k if k not in keys_merchandising_alias else keys_merchandising_alias[k])
            except UnicodeEncodeError:
                if k in title:
                    if include_manufacturer:
                        return "%s %s" % (k if k not in keys_merchandising_alias else keys_merchandising_alias[k], manufacturer)
                    else:
                        return "%s" % (k if k not in keys_merchandising_alias else keys_merchandising_alias[k])
                
        return "VARIOS %s" % (manufacturer)
    else:

        master_separators = [" VOL ", " vol ", " VOL.", " vol.", " Vol ", 
          " Vol. ", u" nº", u" Nº", u" NÚM. ", u" núm. ", u" NúM. ", u" Núm. ",
          u" NÚM ", u" núm ", u" NúM ", u" Núm ", " NUM. ", " num. ", " NuM. ", " Num. ",
          " NUM ", " num ", " NuM ", " Num ",]
        
        for separator in master_separators:
            extra_separators = ":-"
            
            try:
                t = title.split(separator)[0].strip()
                if separator in title:
                    #':' is more
                    for s in extra_separators:
                        if s in t and t.index(s) > 3:
                            t = title.split(separator)[0].split(s)[0]
                    return t
                    
            except UnicodeDecodeError:
                t = title.decode("utf-8").split(separator)[0].strip()
                if separator in title.decode("utf-8"):
                    #':' is more
                    for s in extra_separators:
                        if s in t and t.index(s) > 3:
                            t = title.decode("utf-8").split(separator)[0].split(s)[0]
                    return t
        
        #~ print "\t\t-"
        #try with separator chars
        exceptions = ["-x", "-X"]
        
        
        first_word = re.findall(r"[\w']+", title)[0]

        #exclude first word
        if len(first_word) < 4:
            domain = title[4:]
        else:
            domain = title if any(v in first_word.lower() for v in u"aeiouáéíóú") else title.replace(first_word, "", 1)
          
        #~ print title.split()[0].lower()
        #~ print "\t", title, ":::", domain
         
        
        if not any(e in domain for e in exceptions):
            for sep in ".-:":
                if sep in domain[1:]:
                    title_collection = title.split(sep)[0].strip()
                    i = 1
                    try:
                        while len(title_collection) < 4 and len(title.split(sep)) > 2:
                            title_collection = ".".join(title.split(sep)[:i]).strip()
                            i+=1
                    except IndexError:
                        pass
                    
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
        
        
        gram_exceptions = ["4 fantasticos"]
        if any(title_prenumber.replace(title_collection, "").strip().lower().startswith(ex) for ex in gram_exceptions):
            title_collection = title_prenumber
                    
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
        
def normalize_id(s):
            chars = '/"'
            for c in chars:
                s = s.replace(c,"")
            return s.encode('ascii','ignore')

def clean_spaces(s):
    s = ' '.join(s.splitlines())
    while "\t" in s:
        s = s.replace("\t", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    
    return s.strip()
    
def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')
    
def normalize_content(s):
    try:
        return s.encode('ascii','ignore')
    except:
        return s

MONTHS = {"enero" : 1, "febrero" : 2, "marzo" : 3, "abril" : 4, "mayo" : 5, 
  "junio":6, "julio" : 7, "agosto" : 8, "septiembre" : 9, "octubre" : 10,
  "noviembre" : 11, "diciembre" : 12}

def month2number(month):
    return MONTHS[month.lower()]

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
        "PATRULLA-X VS 4 FANTASTICOS & VENGADORES  (MARVEL GOLD)" : [False, "PATRULLA-X VS 4 FANTASTICOS & VENGADORES"],  
        "DR. INUGAMI (MARUO) (EDICION CARTONE)" : [False, "DR. INUGAMI"],  
        "MALVIVIENDO: EL TEBEO VOL 3" : [False, "MALVIVIENDO"],  
        "ASGARD Edición integral" : [False, "ASGARD"],  
        "20th Century Boys nº 01/22" : [False, "20th Century Boys"],  
        "S.W. Omnibus: Relatos Jedi nº 02" : [False, "S.W. Omnibus"],  
        "ALACK SINNER Nº01/8" : [False, "ALACK SINNER"],  
        "Detective Conan Vol. II Nº 79" : [False, "Detective Conan"],  
        "BLAKE Y MORTIMER 09. EL SECRETO DEL ESPADÓN (1ª PARTE)  PERSECUCIÓN FANTÁSTICA" : [False, "BLAKE Y MORTIMER"],  
        u"Robert Kirkman presenta Witch Doctor-A golpe de bisturí nº 01".upper() : [False, "Robert Kirkman presenta Witch Doctor".upper()],  
        u"S.W. Enciclopedia de personajes".upper() : [False, "S.W. Enciclopedia de personajes".upper()],  
        u"S.W. Imperio Carmesi. Imperio Perdido".upper() : [False, "S.W. Imperio Carmesi".upper()],  
        u"D-LIVE !! Nº01".upper() : [False, "D-LIVE !!".upper()],  
        u"Batman (reedición trimestral) núm. 01".upper() : [False, "BATMAN".upper()],  
        u"Grandes autores de Batman - Grant Morrison y Dave McKean: Asilo Arkham".upper() : [False, "Grandes autores de Batman".upper()],  
        u"Backing Boards Comic Concept para comic: Tamaño CURRENT".upper() : [False, "Backing Boards Comic Concept para comic".upper()],  
        "Mc FARLANE/MAGGIE FIGURA" : [True, "FIGURAS Mc FARLANE"],  
        "NECA/GOLLUM FIGURA 5 CM SCALERS SERIE 1" : [True, "FIGURAS NECA"],  
        "NECA/GOLLUM FIG 5 CM SCALERS SERIE 1" : [True, "FIGURAS NECA"],  
        "SD TOYS/AS HIGH AS HONOR ARRYN CAMISETA CHICA T" : [True, "CAMISETA SD TOYS"],  
        "SD TOYS/CORPSE BRIDE SET A PUNTO DE LIBRO MAGNETICO LA NOVIA CADAVER" : [True, "VARIOS SD TOYS"],  
        "FUNKO/BIKER SCOUT FIG.10 CM VINYL POP STAR WARS" : [True, "FIGURAS FUNKO"],  
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
    


