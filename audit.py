"""
Your task in this exercise has two steps:

- audit the OSMFILE and change the variable 'mapping' to reflect the changes needed to fix 
    the unexpected street types to the appropriate ones in the expected list.
    You have to add mappings only for the actual problems you find in this OSMFILE,
    not a generalized solution, since that may and will depend on the particular area you are auditing.
- write the update_name function, to actually fix the street name.
    The function takes a string with street name as an argument and should return the fixed name
    We have provided a simple test so that you see what exactly is expected
"""
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

#OSMFILE = "example.osm"
#OSMFILE = "example_sf.osm"
OSMFILE = "san-francisco.osm"

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
dog_re = re.compile(r"(dog)|(\bpup)|(\bpet(s|\b|\'))|(animal)|(canine)|(k9)|(\bvet(erinary|\b))", re.IGNORECASE)
not_dog_re = re.compile(r'(hot[\w]?dog)|(dogwood)|(pup(a|u))|(doge)|(popup)|(dogpatch)|(mad\wdog)|(box\wdog)', re.IGNORECASE)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Crescent", "Center", "Highway", "Plaza", "Square", "Way"]

mapping = { "ave" : "Avenue",
            "Ave" : "Avenue",
            "Ave.": "Avenue",
            "Blvd"      : "Boulevard",
            "Blvd,"     : "Boulevard",
            "Blvd."     : "Boulevard",
            "Boulavard" : "Boulevard",
            "Boulvard"  : "Boulevard",
            "Ctr" : "Center",
            "Cntr" : "Center",
            "Ct" : "Court",
            "Ct." : "Court",
            "Cres": "Crescent",
            "Dr" : "Drive",
            "Dr.": "Drive",
            "Ln" : "Lane",
            "Ln.": "Lane",
            "Hwy" : "Highway",
            "Pkwy" : "Parkway",
            "Pl" : "Place",
            "Plz" : "Plaza",
            "Rd" : "Road",
            "Rd.": "Road",
            "St"    : "Street",
            "St."   : "Street",
            "Steet" : "Street"
           }

default_city = "San Francisco"
city_mapping = { "San Francicsco" : "San Francisco",
                 "San Francisco, Ca": "San Francisco",
                 "San Francisco, Ca 94102": "San Francisco",
                 "San Francscio": "San Francisco",
                 "Okaland": "Oakland",
                 "Oakland Ca": "Oakland",
                 "Oakland, Ca": "Oakland",
                 "East Palo Alto": "Palo Alto",
                 "Berkeley, Ca" : "Berkeley"
               }

dog_srch_keys = {"name","name_1","alt_name","old_name","tiger:name_base",\
                 "park:type","amenity","barrier","animal","designation",\
                 "leisure","place","shop","description","comment","note"}
dog_include_keys = {"park","park:type","animal","dog","grooming",\
                    "animal_shelter:adoption","animal_shelter"}
not_dog_amenities = {'fast_food','pub','restaurant','cafe','parking'}

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        
        if street_type not in expected:
            street_types[street_type].add(street_name)
        #else: print 'Correct %s'%(street_name)
    else:
        print 'Found nothing for: %s'%(street_name)


def audit_city_name(city_dict, city_name):
    city_dict.update({city_name:city_dict.get(city_name,0)+1})
    
    
def audit_dog(dog_dict, elem):
    """Expect defaultdict for trying to access dog_dict"""
    add_dict = {}
    non_leisure_park = 0
    debug = None
    for tag in elem.iter("tag"):
        key = tag.attrib['k'].lower()
        if key in dog_srch_keys:
            if key == 'amenity' and tag.attrib['v'] in not_dog_amenities:
                return
            if key == 'name' and re.search('Tot Lot',tag.attrib['v']) is not None:
                # Ignore places with 'Tot Lot' (kid playground) in their names
                return
            if key == 'shop' and (tag.attrib['v'] == 'bicycle' or tag.attrib['v'] == 'supermarket' or tag.attrib['v'] == 'convenience'):
                return
            if key == 'note' and re.search('No dogs',tag.attrib['v']) is not None:
                # Ignores 'No dogs permitted. Open to the public, but garden plots are limited to Parkmerced residents.'
                return
            if is_leisure_park(tag) == False:
                # Ignore elem's with no other valid fields besides {'k': 'leisure', 'v': 'park'}
                non_leisure_park = non_leisure_park + 1
            add_dict[key] = tag.attrib['v']
            #if key == 'amenity': debug = 1
    # Add count of key/value pairs to dog_dict
    if non_leisure_park > 0:
        for key,val in add_dict.items():
            # Consolidate similar keys
            if key == 'alt_name' or key == 'old_name' or key == 'name_1' or key == 'tiger:name_base':
                key = 'name'
            if key == 'note' or key == 'comment':
                key = 'description'
            dog_dict[key].update({val : dog_dict[key].get(val,0)+1})
        if debug: pprint.pprint(add_dict)
            
    
    
def is_dog_related(elem):
    """Looks for tags that might indicate dog friendly locales"""
    if is_leisure_park(elem):
        return True
    if elem.attrib['k'] in dog_include_keys:
        return True
    sub_val = re.sub(not_dog_re,'',elem.attrib['v'],re.IGNORECASE)
    if elem.attrib['k'] in dog_srch_keys and dog_re.search(sub_val) is not None:
        #print elem.attrib
        return True
    return False


def is_leisure_park(elem):
    if elem.attrib['k'] == 'leisure' and (elem.attrib['v'] == 'park' or elem.attrib['v'] == 'park_'):
        return True
    else:
        return False
        

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def is_city(elem):
    return (elem.attrib['k'] == "addr:city" and re.search(r'[a-zA-Z]+',elem.attrib['v']) is not None)


def is_addr_prefix(elem):
    if len(elem.attrib['k']) >= 5 and elem.attrib['k'][:5] == 'addr:':
        #print elem.attrib['k']
        return True
    else:
        return False


def normalize_capitalization(val):
    """Removes leading/trailing whitespaces and 
    capitalizes 1st letter of each word (numbers unchanged)"""
    return ' '.join(word[0].upper() + word[1:] for word in val.strip().lower().split())

def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    city_names = defaultdict(set)
    dog_dict = defaultdict(dict)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            found_dog = False
            for tag in elem.iter("tag"):
                if tag == 'name':
                    tag.attrib['v'] = normalize_capitalization(tag.attrib['v'])
                if is_dog_related(tag):
                    found_dog = True
                if is_addr_prefix(tag):
                    tag.attrib['v'] = normalize_capitalization(tag.attrib['v'])
                    if is_street_name(tag):
                        audit_street_type(street_types, tag.attrib['v'])
                    elif is_city(tag):
                        audit_city_name(city_names, tag.attrib['v'])
            if found_dog == True:
                audit_dog(dog_dict, elem)

    return street_types, city_names, dog_dict


def update_name(name, mapping):

    m = street_type_re.search(name)
    if m:
        street_type = m.group()
        # Handles 'Francisco': set(['14th St, San Francisco']) entry
        # Handles 'Floor': set(['Montgomery Street, 2nd Floor']) entry
        if street_type == 'Francisco' or street_type == 'Floor':
            remove_end = re.search('(.)+,',name)
            if remove_end:
                # group() includes the ',' as the final character, remove it too
                return update_name(remove_end.group(0)[:-1], mapping)
        if street_type in mapping.keys():
            name = name[:len(name)-len(street_type)] + mapping[street_type]

    return name

def update_city(name, mapping):
    if name in mapping.keys():
        name = mapping[name]
    return name

def test():
    st_types, cty_names, dog_tags = audit(OSMFILE)
    
    pprint.pprint(dict(dog_tags))
    
    updated_street = {}
    for st_type, ways in st_types.iteritems():
        street_list = []
        for name in ways:
            better_name = update_name(name, mapping)
            street_list.append(better_name)
            #print name, "=>", better_name
            if name == "9th St.":
                assert better_name == "9th Street"
            if name == "14th St, San Francisco":
                assert better_name == "14th Street"
        updated_street[st_type] = street_list
    #pprint.pprint(dict(st_types))
    pprint.pprint(updated_street)
    
    updated_city = {}
    for city,num in cty_names.items():
        correct_city = update_city(city, city_mapping)
        updated_city[correct_city] = num
        #print city, "=>", correct_city
        if city == "South San Francisco":
            assert correct_city == "San Francisco"
    #pprint.pprint(dict(cty_names))
    #pprint.pprint(updated_city)
    import operator
    # Convert dict to sorted list
    sorted_cities = sorted(updated_city.iteritems(), key=operator.itemgetter(1), reverse=True)
    pprint.pprint(sorted_cities)

if __name__ == '__main__':
    test()