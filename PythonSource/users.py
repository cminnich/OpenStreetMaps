#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import pprint
import re
"""
Your task is to explore the data a bit more.
The first task is a fun one - find out how many unique users
have contributed to the map in this particular area!

The function process_map should return a set of unique user IDs ("uid")
"""

#OSMFILE = "example.osm"
#OSMFILE = "example_sf.osm"
OSMFILE = "san-francisco.osm"


def process_map(filename):
    users = {}
    for _, element in ET.iterparse(filename):
        if 'user' in element.attrib.keys():
            username = element.attrib['user']
            users.update({username : users.get(username,0)+1})

    return users


def test():

    users = process_map(OSMFILE)
    pprint.pprint(users)
    print 'Total # of Users: %d'%(len(users.keys()))
    
    # Sort dictionary on value (# user mentions)
    num_ranked = 10
    print 'Top %d contributors'%(num_ranked)
    import operator
    # Convert dict to sorted list
    sorted_users = sorted(users.iteritems(), key=operator.itemgetter(1), reverse=True)
    #print sorted_users
    total_user_mentions = sum([int(v) for (k,v) in sorted_users])
    ranked_total = 0
    for i in range(num_ranked):
        print "%d) %s, %d updates (%.1f%%)"%(i+1,sorted_users[i][0],sorted_users[i][1],\
            float(sorted_users[i][1])/float(total_user_mentions)*100.0)
        ranked_total += sorted_users[i][1]
    print "%.1f%% of Users contribute %.1f%% of updates"%(float(num_ranked)/float(len(users.keys()))*100.0,\
        float(ranked_total)/float(total_user_mentions)*100.0)
    
    print 'Character encoding test...'
    uni_val = u'Wilfredo S\xe1nchez'
    if uni_val in users.keys():
        print "%s had %d entries"%(uni_val,users[uni_val]) # prints 'Wilfredo Sánchez'
    uni_val = u'\u041c\u0438\u043b\u0430\u043d \u0408\u0435\u043b\u0438\u0441\u0430\u0432\u0447\u0438\u045b'
    if uni_val in users.keys():
        print "%s had %d entries"%(uni_val,users[uni_val]) # prints 'Милан Јелисавчић'

if __name__ == "__main__":
    test()