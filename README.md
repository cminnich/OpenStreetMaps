OpenStreetMaps
==============
Shape/Clean/Analyze San Francisco OpenStreetMap data using Python and MongoDB.

This Python based project takes OpenStreetMap data, specifically for the entire
city of *San Francisco* ([download the .osm file here](http://metro.teczno.com/#san-francisco)), 
and performs some cleaning, shaping, auditing, and analysis of the data.  

There are two files that make up the meat of this project: 

    data.py
    mongo_audit.py

I did this project for the Udacity course: [Data Wrangling with MongoDB] (https://www.udacity.com/course/ud032).
Highly recommend taking it to anyone that's interested.

Reading Data into MongoDB
-------------------------
The data.py file handles reading in the file,
parsing and performing all the data cleaning and shaping.  

Requires MongoDB to be installed, configured and running.  I start my MongoDB instance locally with the following command
    `mongod --dbpath ~/data/db/`

Then in the data.py file, make sure OSMFILE is set to the correct Open Street Map dataset (including correct
relative path).  Run this to read in, clean, shape, and insert data into MongoDB
    `python data.py`
    
Using the fairly large San Francisco dataset, I was running into memory issues
using the ElementTree iterparse function.  The problem was that iterparse does not
free the references to nodes from each iteration.  It continues to build up an in-memory
tree of the entire document, which can drag processing to a halt near the end of the file.
Great reference article for more details by [effbot](http://effbot.org/elementtree/iterparse.htm).

My solution was to constantly clear each element from memory as soon as it was done and saved into
MongoDB.  Rather than build up an entire cleaned/shaped collection and saving to a .json file to
be read into mongoDB later (as initially laid out in the class), I opted for the incremental read of
each node and skip the .json conversion step altogether.

Analyzing with PyMongo
----------------------
The mongo_audit.py file handles the querying and analysis of the Open Street Map data
within MongoDB (using Pymongo).
A variety of functions are available to query the database in a number of ways:

* basic querying
    * searching for names, locations, and nodes
    * using indexes so these queries run fast, even with the large dataset
    * regex based name searching
    * additionally maps any node_refs found within 'ways' elements 
      to corresponding node locations
* aggregation pipelines
    * matching any dog-related nodes (i.e. parks, animal shelters, vets, etc.)
    * additionally can run an aggregation using $geoNear to find dog-related
      nodes that are closest to a given lat/long
    * mapreduce
        * finds the top contributing users, and then finds the most popular fields that
          user has written (i.e. user 'x' fills out the building/amenity/etc. key
          the most frequently)
        * has an option to create a new collection with the results (which would save time
          in the future as subsequent calls could query this collection) or return a dictionary
          of results in-line (but runs somewhat slowly since map_reduce is in javascript, 
          & can't be improved speed-wise by indexes)
        * option to add a query to the map_reduce call to only pass dog-related
          elements to the mapping task (query does use indexes so it'll work fast)

I have a number of queries set up to run automatically by running: 
`python mongo_audit.py`
Or within the Python shell, here is an example of how to create the indexes, search for any names close to 'Dog Park' and find nearby dog-friendly places based on [longitude, latitude]:

    >>> import mongo_audit as ma
    >>> docs = ma.get_collection()
    >>> ma.create_indexes(docs)
    >>> dog_parks = ma.find_name("Dog Park", docs, limit_results=5, printout=1)
    >>> nearby_dog_friendly = ma.dog_related(docs, [-122.39044189454,37.776148988564], near_limit=5)
    >>> import pprint
    >>> pprint.pprint(nearby_dog_friendly)

Other
-----
The other files perform some auditing of the *.osm file without using MongoDB.

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
Within the ['tiger:' prefix](http://wiki.openstreetmap.org/wiki/TIGER),
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


