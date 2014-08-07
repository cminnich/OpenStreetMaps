OpenStreetMaps
==============
Shape/Clean/Analyze San Francisco OpenStreetMap data using Python and MongoDB

This python based project takes OpenStreetMap data, specifically for the entire
city of San Francisco (download the .osm file from 
http://metro.teczno.com/#san-francisco), and performs some cleaning, shaping,
auditing, and analysis of the data.  

The data.py file handles reading the file,
parsing and performing all the data cleaning.  It will either write the shaped
data to a .json file so it can be later imported to MongoDB in a batch process,
or it iteratively inserts each element as it is read from the tree.  This
iterative method of reading an element and storing it to MongoDB before clearing
it is preferred, especially with the large dataset (and the requisite memory needs).

The mongo_audit.py file handles the querying and analysis of within MongoDB (using Pymongo).
A variety of functions are available to query the database in a number of ways:
    + basic querying of names, locations, and nodes 
        - using indexes so these queries run very fast!
    + aggregation pipelines
        - matching any dog-related nodes (i.e. parks, animal shelters, vets, etc.)
        - additionally runs an aggregation using $geoNear to find dog-related
          nodes that are closest to a given lat/long
    + mapreduce (in javascript so indexes can't improve their speed)
        - finds the top contributing users, and then finds the most popular fields that
          user has written to (i.e. building, amenity, etc.)
        - query (which does use indexes) added to the map_reduce call to only pass
          dog-related elements to the mapping task
    + regex based name searching
        - maps any node_refs found within 'ways' elements to corresponding node
          locations

The other files perform some auditing of the *.osm file as follows.

The mapparser.py produces a count of the number of times each tag was seen, i.e:
    {'bounds': 1,
     'member': 28761,
     'nd': 3529134,
     'node': 2905645,
     'osm': 1,
     'relation': 3073,
     'tag': 1133688,
     'way': 312933}

The tags.py file searches 'tags' elements and categorizes them based on a number
of regular expressions.  The following is a summary of the san-francisco.osm dataset:
    {'lower': 625560, 'lower_colon': 506977, 'other': 937, 'problemchars': 214}
Realized significant improvement in allowable tag strings (decreasing 'other')
by specifying a number of domain specific regular expressions.
Within the 'tiger:' prefix (http://wiki.openstreetmap.org/wiki/TIGER),
I found a number of fields ending in a number (i.e. '_2' at the end of the key name)
which represented duplicate keys (or multiple values for the same key).

The users.py file produced a count (by iteratively parsing the .osm file) of all
the users and corresponding number of contributions.
Example output from the san-francisco.osm dataset:
    Total # of Users: 1569
    Top 10 contributors (0.6% of Total Users) contribute 78.6% of updates
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
I additionally found that some usernames were using non-standard characters, to test how
they would display, I printed the following:
    Character encoding test...
    Wilfredo Sánchez had 23 entries
    Милан Јелисавчић had 1 entries
Non-standard characters accounted for
"Wilfredo Sánchez" printed & stored as u'Wilfredo S\xe1nchez'
"Милан Јелисавчић" printed & stored as u'\u041c\u0438\u043b\u0430\u043d \u0408\u0435\u043b\u0438\u0441\u0430\u0432\u0447\u0438\u045b'

The audit.py file was used to perform some initial cleaning of the data without
working in MongoDB.  I created the initial regular expressions to capture as many
dog-related elements as possible (while preventing elements like fast food restaurants
and non-dog related places such as anywhere in 'Dogpatch' from getting in the mix).
Did a variety of other cleaning tasks as follows:
Trimmed (leading/trailing) spaces from key, converted to lower case,
and then capitalized the 1st letter of each word ('v' value).
Remove trailing ', San Francisco' and ', 2nd Floor' from street name to audit correctly.
This shows the original last word (assumed to be street type) and all the updated
street names after being fixed.  
    {'Ave': ['Greenwood Avenue',
             'Earl Avenue',
             'West Portal Avenue',
             ...
             'Day Avenue',
             'Van Ness Avenue',
             'Jerrold Avenue',
             'California Avenue',
             'W & E Of Us 101 N Of Seminary Avenue'],
     'Ave.': ['Menalto Avenue',
              'Jefferson Avenue',
              'Hamilton Avenue',
              'Fairmount Avenue',
              '7th Avenue',
              'Edes Avenue',
              'Holloway Avenue',
              'San Carlos Avenue',
              'Cortland Avenue',
              'Lincoln Avenue',
              'Dartmouth Avenue'],
     'Blvd': ['International Boulevard',
              'Sawyer Camp Trail & Hillcrest Boulevard',
              'Macarthur Boulevard',
              'Anza Boulevard',
              ...
              '380 Foster City Boulevard',
              '10500 Foothill Boulevard',
              'N California Boulevard',
              '25555 Hesperian Boulevard',
              '600 Leweling Boulevard'],
     'Blvd,': ['Nw Quad I-280 / Sr 35 Ic @ Jct Hayne Rd, Golf Course Dr, Skyline Boulevard'],
     'Blvd.': ['Geary Boulevard', 'East Francisco Boulevard'],
     'Boulavard': ['Alemany Boulevard'],
     'Boulvard': ['Alemany Boulevard'],
     ...}
Special case handling of the following:
    'Montomery St, 2nd Floor' updated to 'Montgomery Street'
    '14th St, San Francisco updated to '14th Street'
Audit city names (build count of how many different city's are tagged in addr:city).
Based on the results, specifically the fact that 'San Francisco' is very rarely
tagged as a city name, I'm assuming that 'San Francisco' is implicit in all the 
elements that don't specifically call out a city name in the address field
(and perhaps should be explicitly written to all elements without a specified city name).
Corrected slight differentiations in city spellings (i.e. from typos
or including CA and zip code, or just wrong entries like '155'). 
Here is the ordered breakdown of (corrected) city names, with the # of mentions:
    [('Redwood City', 23143),
     ('Oakland', 317),
     ('Union City', 252),
     ('Burlingame', 157),
     ('Walnut Creek', 151),
     ('Albany', 70),
     ('Alameda', 60),
     ('San Mateo', 48),
     ...]
Added auditing for dog specific tags - including any leisure parks, or fields
with dog related values (using regex values like dog, pup, vet, etc.), or
specific key values (see dog_include_keys like park, dog, grooming, etc).
Names of (possibly) dog-related entries:
         {'10th Avenue & Clement Mini Park': 1,
          '1315 18th Street Parklet': 1,
          '236 Townsend Parklet': 1,
          '24th & York Mini Park': 1,
          '25th Street Mini Park': 1,
          '29th & Diamond Open Space': 2,
          ...
          'Lindsay Wildlife Hospital': 1,
          'Linear Park': 6,
          'Lions Park': 1,
          'Little Ark Grooming Shop': 1,
          'Little Hollywood Park': 1,
          'Littlejohn Park': 1,
          'Live Oak Nature Trail': 1,
          ...}


