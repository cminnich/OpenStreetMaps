#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Use iterative parsing to process the map file and
find out not only what tags are there, but also how many, to get the
feeling on how much of which data is expected within the map.

The output should be a dictionary with the tag name as the key
and number of times this tag can be encountered in the map as value.
"""
import xml.etree.ElementTree as ET
import pprint

#OSMFILE = "../example.osm"
OSMFILE = "../example_sf.osm"
#OSMFILE = "../san-francisco.osm"

def count_tags(filename):
    
    xml_dict = {}
    context = ET.iterparse( filename )
    for event, elem in context:
        if elem.tag in xml_dict:
            xml_dict[elem.tag] = xml_dict[elem.tag] + 1
        else:
            xml_dict.update({elem.tag : 1})
    return xml_dict

def test():

    tags = count_tags(OSMFILE)
    pprint.pprint(tags)


if __name__ == "__main__":
    test()