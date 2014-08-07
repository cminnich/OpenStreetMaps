#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import pprint
import re
"""
Explores the data a bit more.
First finds out how many unique users
have contributed to the map in this particular area!

The function process_map should return a set of unique user IDs ("uid")
{'#smolsen': 1,
 '-MegaByte-': 268,
 '-jha-': 5,
 '123maps': 11,
 '1248': 2,
 '25or6to4': 4,
 '415vince': 1,
 '42429': 9323,
 'AE35': 103,
...
 'zoeplankton': 230,
 'zors1843': 7,
 'zoverax': 426,
 'zshipko': 11,
 u'\u041c\u0438\u043b\u0430\u043d \u0408\u0435\u043b\u0438\u0441\u0430\u0432\u0447\u0438\u045b': 1}
Total # of Users: 1569
Top 10 contributors
1) ediyes, 731906 updates (22.7%)
2) Luis36995, 561664 updates (17.4%)
3) Rub21, 424249 updates (13.2%)
4) oldtopos, 337987 updates (10.5%)
5) KindredCoda, 139788 updates (4.3%)
6) DanHomerick, 117895 updates (3.7%)
7) nmixter, 75394 updates (2.3%)
8) dchiles, 54264 updates (1.7%)
9) oba510, 46686 updates (1.4%)
10) StellanL, 42732 updates (1.3%)
0.6% of Users contribute 78.6% of updates
Character encoding test...
Wilfredo Sánchez had 23 entries
Милан Јелисавчић had 1 entries
"""

#OSMFILE = "../example.osm"
#OSMFILE = "../example_sf.osm"
OSMFILE = "../san-francisco.osm"


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
