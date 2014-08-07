#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Wrangling the data and transform the shape of the data
into the following model.
Output should be a list of dictionaries that look like this example:

{
"id": "2406124091",
"type: "node",
"visible":"true",
"created": {
          "version":"2",
          "changeset":"17206049",
          "timestamp":"2013-08-03T16:43:42Z",
          "user":"linuxUser16",
          "uid":"1219059"
        },
"pos": [41.9757030, -87.6921867],
"address": {
          "housenumber": "5157",
          "postcode": "60625",
          "street": "North Lincoln Avenue"
        },
"amenity": "restaurant",
"cuisine": "mexican",
"name": "La Cabana De Don Luis",
"phone": "1 (773)-271-5176"
}

Parses the map file, and runs shaping and cleaning functions on the elements.
Cleaning: Standardizes the street name endings and city names
    San Mateo Rd => San Mateo Road
    San Francicsco => San Francisco
Renames the various alternate name keys to be in a single dictionary called 'other_names'.
Returns a dictionary, containing the shaped data for that element.
Either saves the data to a file, so that mongoimport can be used to
import the shaped data into MongoDB, or iteratively adds each element to MongoDB.
The mongo_process_map function should be used to iteratively read large datafiles,
i.e. 'san-francisco.osm' due to the nature of the large file and requisite large
amounts of memory necessary to store the element tree as well as processed results
(i.e. how process_map parses the data).
Update misspelled city names (determined from previous exercise) and standardize
street endings.  Specific keys are excluded from being saved to the output/database,
which have been identified to not add any valuable information (i.e. 'paloalto_ca:id').

In particular the following shaping tasks are be done:
- only processes 2 types of top level tags: "node" and "way"
- all attributes of "node" and "way" should be turned into regular key/value pairs, except:
    - attributes in the CREATED array should be added under a key "created"
    - attributes for latitude and longitude should be added to a "pos" array,
      for use in geospacial indexing. Make sure the values inside "pos" array are floats
      and not strings.
- if second level tag "k" value contains problematic characters, it should be ignored
- if second level tag "k" value starts with "addr:", it should be added to a dictionary "address"
- if second level tag "k" contains a ":" within the text, it should be turned into a dictionary
    - ensures 'reserved' keys are not overwritten (i.e. 'address') 
    
- if there is a second ":" in the key (i.e. for the type/direction of a street),
  the tag should be ignored, for example:

<tag k="addr:housenumber" v="5158"/>
<tag k="addr:street" v="North Lincoln Avenue"/>
<tag k="addr:street:name" v="Lincoln"/>
<tag k="addr:street:prefix" v="North"/>
<tag k="addr:street:type" v="Avenue"/>
<tag k="amenity" v="pharmacy"/>

  should be turned into:

{...
"address": {
    "housenumber": 5158,
    "street": "North Lincoln Avenue"
}
"amenity": "pharmacy",
...
}

- for "way" specifically:

  <nd ref="305896090"/>
  <nd ref="1719825889"/>

should be turned into
"node_ref": ["305896090", "1719825889"]
"""
from pymongo import MongoClient
import xml.etree.ElementTree as ET
import pprint
import re
import codecs
import json
from audit import mapping, city_mapping, normalize_capitalization, \
    street_type_re, update_name, update_city, default_city
from mongo_audit import client, db, get_collection, delete_collection, \
    size_of_collection, check_collection_exists

#OSMFILE = "../example.osm"
OSMFILE = "../example_sf.osm"
#OSMFILE = "../san-francisco.osm"

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
skip_colon = re.compile(r'^((sfgov)|(.*_ca)|(gosm)|(massgis)):')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]
NAMES = [ "alt_name", "name_1", "old_name" ]
RESERVED_KEYS = [ "address", "name", "tiger", "created", "gnis", "pos", "id" ]

def node_way_shape(key, val, node):
    """Helper function to shape key/value pairs in top level node/way tag"""
    # Created array
    if key in CREATED:
        if 'created' not in node.keys():
            node['created'] = {}
        node['created'].update( {key:val} )
    elif key == 'lat' or key == 'lon':
        # Remove spaces from invalid data
        val = float(re.sub('\s','',val.strip()))
        # 'pos' = [Longitude, Latitude]
        if 'pos' not in node.keys():
            node['pos'] = [val]
        elif key == 'lon':
            node['pos'].insert(0, val)
        else: # key == 'lat'
            node['pos'].append(val)
    else:
        #if key != 'id': print "Top level key=%s,val=%s"%(key,val)
        node[key] = val
    #print "key: %s, val: %s"%(key,val)
    return node
    
def colon_clean(key, val, node):
    if len(key) > 5 and key[:5] == 'name:':
        # Create array of alternate names (or names in different languages)
        key = key[5:]
        if 'other_names' not in node.keys():
            node['other_names'] = {key:val}
        else:
            node['other_names'].update( {key:val} )
    elif len(key) > 5 and key[:5] == 'addr:':
        # For all address values, capitalize only the first character of each word
        val = normalize_capitalization(val)
        key = key[5:]
        if lower_colon.search(key) is None:
            if key == "city":
                fixed_city = update_city(val, city_mapping)
                if fixed_city != val: 
                    print val, "=>", fixed_city
                    val = fixed_city
            #Defaults to having 'San Francisco' as city name
            elif "address" not in node.keys():
                node['address'] = {'city':default_city}
            elif "city" not in node['address'].keys():
                node['address'].update({'city':default_city})
            if key == "street":
                fixed_name = update_name(val, mapping)
                if fixed_name != val: 
                    print val, "=>", fixed_name
                    val = fixed_name
            #if debug: print "Address key: %s, val: %s"%(key,val)
            if 'address' not in node.keys():
                node['address'] = {key:val}
            else:
                node['address'].update({key:val})
        # Ignore keys with more than 1 colon (and starting with "addr:")
    elif len(key) > 6 and key[:6] == 'tiger:':
        key = key[6:]
        #if debug: print "Tiger key: %s, val: %s"%(key,val)
        # Skips 'Tiger:MTFCC' keys
        if key == 'mtfcc':
            return node
        if 'tiger' not in node.keys():
            node['tiger'] = {key:val}
        else:
            node['tiger'].update({key:val})
    elif len(key) > 5 and key[:5] == 'gnis:':
        key = key[5:]
        if 'gnis' not in node.keys():
            node['gnis'] = {key:val}
        else:
            node['gnis'].update({key:val})
    elif skip_colon.search(key) is not None:
        # These keys have garbage values, don't store them
        # Examples of keys that are skipped:
        #  'redwood_city_ca:addr_id', 'rwc_ca:buildingid', 'paloalto_ca:id'
        #  'gosm:sig:8CBDE645', 'massgis:cat'
        print 'Skip colon match: %s=%s'%(key,val)
        return node
    else:
        dict_key,nested_key = key.split(":",1) # Only create dict with first part of key
        if nested_key in RESERVED_KEYS:
            # Key 'note:address' contains a street address, save it
            if dict_key == 'note' and nested_key == 'address':
                val = update_name(val, mapping)
                if 'address' not in node.keys():
                    node['address'] = {'street':val}
                elif 'street' not in node['address'].keys():
                    node['address'].update({'street':val})
                else:
                    node['address'].update({'street_address':val})
            # Skipping 'source:name'
            return node
        #if debug: print "Other (%s) key: %s, val: %s"%(key,nested_key,val)
        if dict_key not in node.keys():
            node[dict_key] = {nested_key:val}
        else:
            if isinstance(node[dict_key],dict):
                node[dict_key].update({nested_key:val})
            else:
                # Convert to dict (use orig outer dict_key with '_key'
                # appended as the nested key for the original value
                node[dict_key] = {dict_key+'_key':node[dict_key], nested_key:val}
    return node
    
def tag_shape(key, val, node, debug=False):
    """Helper function to shape key/value pairs in child tag.
    Fix or ignore bad/unwanted keys.
    Standardize val for name keys (first letter of each word capitalized),
    and street and cities names are fixed (typos corrected and abbreviations relaced).
    Keys with colons will be shaped into nested dictionaries, with the remaining string
    after the first colon as the nested dictionary key (part before the colon is 
    the top level key).
    Expects calling function to convert key to lowercase and
    trim leading/trailing spaces."""
    if problemchars.search(key) is None:
        #print "Correct key: %s, val: %s"%(key,val)
        if key[-1] == ':':
            # remove trailing ':' from key
            print 'Fixing %s key -> %s=%s'%(key,key[:-1],val)
            key = key[:-1]
        
        if re.search('name',key,re.IGNORECASE) is not None:
            # For all name keys, capitalize only the first character of each word (value)
            val = normalize_capitalization(val)
        if key == 'name':
            # Handling 'name' since it is in RESERVED_KEYS
            node[key] = val
        elif key in NAMES:
            # Create array of alternate names (or names in different languages)
            if 'other_names' not in node.keys():
                node['other_names'] = {key:val}
            else:
                node['other_names'].update( {key:val} )
        elif key == 'latitude' or key == 'longitude':
            parsed_pos = re.search(r'(-?\w*\d*\.\d*)',val)
            if parsed_pos is not None:
                float_val = float(parsed_pos.group())
                #print '%s->pos: %s->%f'%(key,val,float_val)
                val = float_val
                if 'pos' not in node.keys():
                    node['pos'] = [val]
                elif len(node['pos']) == 1:
                    if key == 'longitude':
                        node['pos'].insert(0,val)
                    else: # 'latitude'
                        node['pos'].append(val)
                    # else: Ignore latitude/longitude (already set by attrib in top level node)
            else:
                print 'WARNING: Regex failed for %s=%s'%(key,val)
        # Handles all keys with a colon
        elif re.search(":",key) is not None:
            return colon_clean(key, val, node)
        else:
            #if debug: print "Adding key: %s, val: %s"%(key,val)
            if key in RESERVED_KEYS:
                if debug: print "Reserved key: %s, val: %s"%(key,val)
                # Change the key or add to dict structure
                if key == 'address' or key == 'created':
                    if key not in node.keys():
                        node[key] = {key+'_key':val}
                    else:
                        node[key].update({key+'_key':val})
                else:
                    node[key+'key'] = val
            else:
                node[key] = val
    else:
        if debug: print "PROBLEM key: %s, val: %s"%(key,val)
    return node


def standardize_key(key):
    return re.sub(r'\s','',key.lower().strip())


def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way":
        node['type'] = element.tag
        
        for key,val in element.attrib.items():
            node = node_way_shape(standardize_key(key), val, node)
        
        for tag in element.iter('tag'):
            if 'k' in tag.attrib.keys() and 'v' in tag.attrib.keys():
                node = tag_shape(standardize_key(tag.attrib['k']), tag.attrib['v'], node, True)
            else:
                print "Invalid tag element attrib: %s"%(element.attrib)
        
        for nd in element.iter('nd'):
            if 'ref' in nd.attrib.keys():
                val = nd.attrib['ref']
                #print "ND val: %s"%(val)
                if 'node_refs' not in node.keys():
                    node['node_refs'] = [val]
                else:
                    node['node_refs'].append(val)
                        
        return node
        
    else:
        #print "Other: %s"%(element.tag)
        #if element.tag == 'tag': print 'Tag: %s'%(element.attrib)
        return None


def process_map(file_in, pretty = False):
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

def mongo_process_map(file_in,print_only=None):
    """Iteratively parse file and read into mongoDB incrementally,
    clearing elements as they are read in."""
    context = ET.iterparse(file_in, events=("start", "end"))
    context = iter(context) # make iterator
    event, root = context.next() # get the root element
    root.clear() # clear root (eliminate references to children)
    docs = get_collection()
    for event, elem in context:
        #print 'Reading %s [%s]'%(elem.tag,event)
        if event == "end":
            el = shape_element(elem)
            #pprint.pprint(el)
            if el:
                if print_only:
                    pprint.pprint(el)
                else:
                    docs.insert(el)
                # clear element once end event of node/way has been shaped and read in
                elem.clear()
    return docs
    

def import_to_mongodb(data,clear_all=False):
    #mongod --dbpath /Users/cminnich/data/db/
    #Command line import can be used after .json file is writted
    # mongoimport --dbpath /Users/cminnich/data/db/ -d OpenStreetMaps -c SanFrancisco --file san-francisco.osm.json
    if clear_all:
        delete_collection()
        print "DB Emptied!"
    docs = get_collection()
    print "Before: %d documents"%(docs.find().count())
    docs.insert(data) # Batch insert
    print "After: %d documents"%(docs.find().count())

def test():
    skip_mongo = None # Change to None to add to mongoDB
    if skip_mongo is None:
        if check_collection_exists():
            print 'Deleting existing collection'
            delete_collection()
    docs = mongo_process_map(OSMFILE,skip_mongo)
    if docs:
        print 'Now has %d documents in collection'%(docs.find().count())
    
def test_1():
    """Warning: memory hog for large san-francisco.osm file, use test() instead"""
    data = process_map(OSMFILE, False)
    
    if OSMFILE == "san-francisco.osm":
        
        drop_existing_db = True
        import_to_mongodb(data, drop_existing_db)
        
        if len(data) > 0:
            assert data[0]['created'] == {'changeset': '12135372',
                                          'timestamp': '2012-07-06T22:08:20Z',
                                          'uid': '169004',
                                          'user': 'oldtopos',
                                          'version': '12'}
            assert data[0]['pos'] == [37.4817473, -122.1770085]
            
            docs = get_collection()
            import random
            random.seed()
            pos = None
            while pos is None:
                ind = random.randint(0,len(data)-1)
                print "Random index %d"%(ind)
                if 'id' in data[ind].keys() and 'pos' in data[ind].keys():
                    pprint.pprint(data[ind])
                    pos = data[ind]['pos']
                    matching_id = docs.find_one({'_id' : data[ind]['_id']})
                    pprint.pprint(matching_id)
                    assert matching_id['pos'] == pos
                    
        else:
            print "WARNING: No data!"
    

if __name__ == "__main__":
    test()
