#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import pprint
import re
"""
Your task is to explore the data a bit more.
Before you process the data and add it into MongoDB, you should
check the "k" value for each "<tag>" and see if they can be valid keys in MongoDB,
as well as see if there are any other potential problems.

We have provided you with 3 regular expressions to check for certain patterns
in the tags. As we saw in the quiz earlier, we would like to change the data model
and expand the "addr:street" type of keys to a dictionary like this:
{"address": {"street": "Some value"}}
So, we have to see if we have such tags, and if we have any tags with problematic characters.
Please complete the function 'key_type'.
"""

#OSMFILE = "example.osm"
#OSMFILE = "example_sf.osm"
OSMFILE = "san-francisco.osm"

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
lower_colon_two = re.compile(r'^([a-z]|_)*:([a-z]|_)*:([a-z]|_)*$')
lower_colon_three = re.compile(r'^([a-z]|_)*:([a-z]|_)*:([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

# Allowable tag names
tiger = re.compile(r'^tiger:([a-z0-9]|_)*$')
name_num = re.compile(r'^(name_[0-9])$')
movie_3d = re.compile(r'^([a-z]|_)*:(3d)$')

def key_type(element, keys):
    if element.tag == "tag":
        #print element.attrib
        
        ## Convert keys to lower case and replace ' ' with '_'
        element.attrib['k'] = element.attrib['k'].lower().replace(" ", "_")
        
        lo_re = lower.match(element.attrib['k'])
        loco_re = lower_colon.match(element.attrib['k'])
        loco2_re = lower_colon_two.match(element.attrib['k'])
        loco3_re = lower_colon_three.match(element.attrib['k'])
        loco_srch_re = lower_colon.search(element.attrib['k'])
        pro_re = problemchars.search(element.attrib['k'])
        tiger_re = tiger.match(element.attrib['k'])
        name_num_re = name_num.match(element.attrib['k'])
        movie_3d_re = movie_3d.match(element.attrib['k'])
        
        is_prob = None
        
        if pro_re:
            is_prob = 1 # Prevents being marked again within other
            key = element.attrib['k']
            if pro_re.group(0) == '.':
                ## Handle '.' in website name
                # k=sfgov.org:OFFICE_TYP : v=Post Office
                if len(key) >= 9 and key[:9] == 'sfgov.org':
                    is_prob = None
                    element.attrib['k'] = element.attrib['k'].replace('.org','_website')
                    lo_re = lower.match(element.attrib['k'])
                    loco_re = lower_colon.match(element.attrib['k'])
                    loco2_re = lower_colon_two.match(element.attrib['k'])
                    loco3_re = lower_colon_three.match(element.attrib['k'])
                    loco_srch_re = lower_colon.search(element.attrib['k'])
                    pro_re = problemchars.search(element.attrib['k'])
            if is_prob is not None:
                keys.update({'problemchars':keys.get('problemchars',0)+1})
                #pro_re = problemchars.sub(element.attrib['k'],'')
                print 'Problem: %s; k=%s : v=%s'%(pro_re.group(),element.attrib['k'],element.attrib['v'])
                
        if lo_re or name_num_re:
            keys.update({'lower':keys.get('lower',0)+1})
            #print 'Lower: ',lo_re.group()
        elif loco_re or loco2_re or loco3_re or tiger_re or movie_3d_re:
            keys.update({'lower_colon':keys.get('lower_colon',0)+1})
            #if tiger_re: print 'tiger:%s; k=%s : v=%s'%(loco_srch_re.group(),element.attrib['k'],element.attrib['v'])
            #if loco2_re: print '2 Colons: %s : v=%s'%(loco2_re.group(),element.attrib['v'])
            #if loco3_re: print '3 Colons: %s : v=%s'%(loco3_re.group(),element.attrib['v'])
            #print 'Colon: %s : v=%s'%(loco_re.group(),element.attrib['v'])
        elif loco_srch_re:
            keys.update({'mult_lower_colon':keys.get('mult_lower_colon',0)+1})
            print 'Multiple Colons: %s; k=%s : v=%s'%(loco_srch_re.group(),element.attrib['k'],element.attrib['v'])
        elif is_prob is None:
            ## Handle '_1'
            # name_1=Lilly Street
            #   Array of names (if different)
            # k=tiger:zip_left_1 : v=94117
            # k=tiger:zip_left_2 : v=94117
            #   Remove duplicate zips (check zip_left == zip_right == zip_left_1 == ...)
            keys.update({'other':keys.get('other',0)+1})
            print 'Other: k=%s : v=%s'%(element.attrib['k'],element.attrib['v'])
        
    return keys



def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys



def test():
    # You can use another testfile 'map.osm' to look at your solution
    # Note that the assertions will be incorrect then.
    keys = process_map(OSMFILE)
    pprint.pprint(keys)
    #assert keys == {'lower': 5, 'lower_colon': 0, 'other': 2, 'problemchars': 0}


if __name__ == "__main__":
    test()