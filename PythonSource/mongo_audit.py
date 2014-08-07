"""
Imports the json dump data into mongoDB.  Expects data.py to have generated
the json dump of the shaped and cleaned *.osm data.

Start mongoDB server locally
mongod --dbpath /Users/cminnich/data/db/

Work within the shell example
>>> import mongo_audit as ma
>>> docs = ma.get_collection()
Search for a specific string in a name (converted to regex), and any ways that
are found will have their 'node_refs' expanded out (by searching for corresponding
nodes that match id's.
>>> ma.find_name("Dog Park", docs, limit_results=5, printout=1)
Runs a set of mongoDB queries (using aggregation pipeline and mapreduce
for various queries)
>>> ma.test()

Tested the indexes created are being used by appending the .explain()
function on a query and verifying that index was being used (i.e. BTreeCursor)
When searching by user first:
    u'cursor': u'BtreeCursor created.user_1_name_1'
When using regex to search by name (find_name function):
    u'cursor': u'BtreeCursor name_1'

Running test() produces the following:
San Francisco has 3218578 documents
Searching for the closest 5 dog-related elements
[{u'_id': ObjectId('53e1e612303db204050f6233'),
  u'created': {u'changeset': u'11248664',
               u'timestamp': u'2012-04-10T07:33:06Z',
               u'uid': u'116029',
               u'user': u'Gregory Arenius',
               u'version': u'2'},
  u'distance': 0.010814452644951072,
  u'ele': u'1',
  u'gnis': {u'county_id': u'075',
            u'created': u'11/01/1994',
            u'feature_id': u'1655591',
            u'state_id': u'06'},
  u'id': u'358803501',
  u'leisure': u'park',
  u'name': u'Agua Vista Park',
  u'pos': [-122.3861326, 37.7662302],
  u'type': u'node'},
  ...]
Aggregation Result (Dog related documents near Lat:37.78, Lon:-122.39) Count: 5
Top 10 contributing users (out of 174) in dog_user_count:
1) iandees [254]
2) oba510 [108]
3) StellanL [107]
4) nmixter [96]
5) dchiles [93]
6) Gregory Arenius [63]
7) DanHomerick [61]
8) KindredCoda [49]
9) oldtopos [35]
10) JessAk71 [31]
Top 5 (non-required) fields iandees has entered and the associated count
  1) name : 970
  2) amenity : 543
  3) leisure : 270
  4) natural : 61
  5) waterway : 53
  iandees created 972 total documents
 iandees created 254 dog-related entries with the following names (only displaying 5)
  Agate Beach County Park
  Alamo Square Historic District
  Alto Bowl Preserve
  Alvarado Park
  Alvarado Park
Top 10 contributing users (out of 1561) in user_count:
1) ediyes [731437]
2) Luis36995 [561610]
3) Rub21 [423726]
4) oldtopos [337873]
5) KindredCoda [139772]
6) DanHomerick [117745]
7) nmixter [75346]
8) dchiles [53924]
9) oba510 [46597]
10) StellanL [42709]
Top 5 (non-required) fields ediyes has entered and the associated count
  1) building : 62778
  2) address : 463
  3) source : 344
  4) name : 288
  5) amenity : 177
  ediyes created 731437 total documents
"""
import pymongo
import pprint
import re
from audit import dog_re, dog_include_keys, not_dog_amenities

any_char_re = re.compile(r".*\w+.*")
dog_re = re.compile(r"(dog(?!patch))|(\bpup)|(\bpet(s|\b|\'))|(animal)|(canine)|(k9)|(\bvet(erinary|\b))", re.IGNORECASE)
not_dog_amenities_re = re.compile(r"^((fast_food)|(pub)|(restaurant)|(cafe)|(parking))$", re.IGNORECASE)
not_dog_names_re = re.compile(r"^.*rec(reation)?\s*ce?nte?r.*$", re.IGNORECASE)
dog_park_re = re.compile(r".*((park)|(dog)).*", re.IGNORECASE)

std_key_list = [ "created", "id", "node_refs", "type", "pos", "ele", "gnis" ]

dog_qry ={"$and":[{ "amenity" : {"$not":not_dog_amenities_re}},
                  { "name" : {"$not":not_dog_names_re}},
                  {"$or":[{ "name" : dog_re },
                          { "amenity" : dog_re },
                          { "leisure" : dog_park_re },
                          { "park" : any_char_re },
                          { "park_type" : any_char_re },
                          { "animal" : any_char_re },
                          { "dog" : r'/.*[yY]es.*/' },
                          { "grooming" : any_char_re },
                          { "animal_shelter" : any_char_re },
                          { "animal_shelter:adoption" : any_char_re}]}]}

client = pymongo.MongoClient("localhost",27017)
db = client['OpenStreetMaps']
default_collection_name = 'SanFrancisco'

def get_collection(collection_name=default_collection_name):
    docs = db[collection_name]
    return docs
    
def check_collection_exists(collection_name=default_collection_name):
    if collection_name in db.collection_names():
        return True
    else:
        return False
    
def delete_collection(collection_name=default_collection_name):
    db.drop_collection(collection_name)

def size_of_collection(docs):
    num_docs = docs.find().count()
    return num_docs
    
def create_indexes(docs):
    # Ensure Index is a wrapper around Create Index, first checks to see
    # if the index exists before creating it. Use
    #   docs.index_information()
    # to see what indexes exist
    docs.ensure_index([("pos", pymongo.GEO2D)])
    docs.ensure_index([("created.user",1),("name",1)])
    docs.ensure_index([("name",1)])
    docs.ensure_index([("id",1)])
    
def get_near_loc(lat,lon,docs):
    query = { "pos" : { "$near" : [ lon, lat ] } }
    #projection = { "_id" : 0, "name" : 1, "pos" : 1, "address" : 1 }
    close_elems = docs.find(query).limit(5)
    return close_elems

def get_nearby_dog_pipeline(near_loc,near_limit):
    agg_query = [{ "$geoNear" : { "near" : near_loc, 
                                  "distanceField" : "distance", 
                                  "query" : dog_qry,
                                  "limit" : near_limit,
                                  "maxDistance" : 0.05 } }]
    return agg_query
    
def get_dog_pipeline():
    aggregate_query = [ { "$match" : {"$and":[{"$or":[{ "pos" : {"$size":2} },
                                                      { "address.street" : any_char_re }] },
                                              { "amenity" : {"$not":not_dog_amenities_re}},
                                              {"$or":[{ "name" : dog_re },
                                                      { "amenity" : dog_re },
                                                      { "leisure" : dog_park_re },
                                                      { "park" : any_char_re },
                                                      { "park_type" : any_char_re },
                                                      { "animal" : any_char_re },
                                                      { "dog" : r'/.*[yY]es.*/' },
                                                      { "grooming" : any_char_re },
                                                      { "animal_shelter" : any_char_re },
                                                      { "animal_shelter:adoption" : any_char_re}]}] } },
                        { "$project" : { "_id" : 0, "name" : 1, "pos" : 1, "address" : 1, \
                                         "amenity" : 1, "leisure" : 1, "park" : 1, "park_type" : 1, \
                                         "animal" : 1, "dog" : 1, "grooming" : 1, "animal_shelter" : 1, "animal_shelter:adoption" : 1}}]
    
    return aggregate_query
    
    
def dog_related(docs,near_loc=None,near_limit=10):
    """If near_loc is a [longitude,latitude] array, searches for the closest 10
    elements in the database that fulfill the dog-related search.
    Otherwise (near_loc=None), leaves the location based aspect out of the 
    aggregate query and just finds documents that:
    + have either pos (lat,long) or address.street field (with 1+ valid characters)
    + amenity field does not match those as specifically identified to be non-dog related
    + match any of the following criteria
        + dog-related name or amenity
        + parks
        + dog-related key fields (i.e. grooming, animal_shelter, vet, etc)
    """
    if near_loc is None:
        agg_query = get_dog_pipeline()
    else:
        agg_query = get_nearby_dog_pipeline(near_loc,near_limit)
    loc_counts = docs.aggregate(agg_query)
    return loc_counts['result']
    
def find_pos(docs):
    """Number of documents with a positions ('pos') key [lat,long].
    Number of documents with a latitude/longitude key.
    [Deprecated] Database should no longer contain latitude/longitude keys (only pos)"""
    query = { "$or" : [{ "pos" :  { "$exists" : 1 } },
                       { "$and" : [{ "latitude"  : {"$exists" : 1}, 
                                     "longitude" : {"$exists" : 1} }
                                   ] } 
                       ] }
    projection = { "_id" : 0, "pos" : 1, "latitude" : 1, "longitude" : 1, "name" : 1 }
    positions = docs.find(query,projection)
    return positions
    
def find_parks(docs):
    # Find parks with a name
    query = { "$and" : [{ "name" : { "$exists" : 1 } },
                        { "$or" : [{ "leisure" : { "$regex" : r"([pP]ark)|([dD]og)" } },
                                   { "name" : dog_re }] }
                       ] }
    projection = { "_id" : 0, "type" : 0, "created" : 0, "id" : 0, "node_refs" : 0 }
    parks = docs.find(query,projection)
    return parks

def find_name(srch_str, docs, limit_results=5, printout=1):
    srch_str = '.*'+re.sub(r'\W','.*',srch_str)+'.*'
    if printout:
        print 'Regex: %s'%(srch_str)
    custom_re = re.compile(srch_str,re.IGNORECASE)
    # query = { "$and" : [{ "pos" :  { "$exists" : 1 } },
                       # { "name" : custom_re }] }
    result_limit = 5
    results = docs.find({ "name" : custom_re },{ "_id" : 0 }).limit(limit_results)
    if printout:
        print 'Top %d Results: %s Search'%(limit_results,srch_str)
    result_list = []
    for r in results:
        if r['type'] == 'way' and 'node_refs' in r.keys():
            r['node_refs'] = node_ref_mapping(docs, r['node_refs'])
        if printout:
            pprint.pprint(r)
        result_list.append(r)
    return result_list

def node_ref_mapping(docs, node_ref_list):
    """Maps the list of node_refs (ids) from way types
    to corresponding node ids.  If found, replaces id in list
    with dictionary of matching node id (w/ only id, pos, and name fields)."""
    mapped_node_refs = []
    for nr in node_ref_list:
        nr_query = { "id" : nr }
        nr_projection = { "_id" : 0, "id" : 1, "pos" : 1, "name" : 1 }
        found_nr = docs.find_one(nr_query, nr_projection)
        if found_nr:
            mapped_node_refs.append(found_nr)
        else:
            mapped_node_refs.append(nr)
    return mapped_node_refs
    
def dog_related_by_user(docs, username, print_names=20):
    """print_names specifies the maximum number of names to print to the screen"""
    user_and_dog_query = {"$and":[{ "created.user" : username }, dog_qry]}
    all_posts = docs.find(user_and_dog_query)
    if print_names is not None:
        print ' %s created %d dog-related entries with the following names (only displaying %d)'%(username,all_posts.count(),print_names)
        for i in all_posts:
            if 'name' in i.keys() and print_names > 0:
                print '  %s'%(i['name'])
                print_names -= 1
    return all_posts
                
def mapreduce_print_user_created_key_count(docs, username):
    """Runs MapReduce to generate a count of all the keys (fields)
    created by this user, then prints out each key and count"""
    from bson.code import Code
    # Note: JS used for mapreduce so indexes will not help to improve performance
    mapper = Code("""
                  function () {
                      for (var key in this) { 
                          emit(key, 1); 
                      }
                  }
                  """)
    reducer = Code("""
                   function (key, values) {
                     var total = 0;
                     for (var i = 0; i < values.length; i++) {
                         total += values[i];
                     }
                     return total;
                   }
                   """)
    user_dict = docs.map_reduce(mapper, reducer, {'inline':1}, query={ "created.user" : username })
    user_tuple = [(keys['_id'],keys['value']) for keys in user_dict['results']]
    import operator
    sorted_user_keys = sorted(user_tuple, key=operator.itemgetter(1), reverse=True)
    num_displayed_allowed = 5
    total_entries = 0
    num_displayed = 1
    print "Top %d (non-required) fields %s has entered and the associated count"%(num_displayed_allowed,username)
    for keys,cnt in sorted_user_keys:
        if keys == '_id':
            total_entries = int(cnt) # Saving '_id' as total since it is in every entry
        elif keys not in std_key_list:
            if num_displayed <= num_displayed_allowed:
                print "  %d) %s : %d"%(num_displayed,keys,int(cnt))
                num_displayed += 1
    print "  %s created %d total documents"%(username,total_entries)
    return user_dict
    
def mapreduce_users(docs,output_inline=1,dog_specific=None):
    from bson.code import Code
    # this.tags.forEach(function(z) {
    mapper = Code("""
                  function () {
                      emit(this.created.user, 1);
                  }
                  """)
    reducer = Code("""
                   function (key, values) {
                     var total = 0;
                     for (var i = 0; i < values.length; i++) {
                       total += values[i];
                     }
                     return total;
                   }
                   """)
    if dog_specific is not None:
        output_to = {"replace":"dog_user_count"}
        user_col = docs.map_reduce(mapper, reducer, output_to, query=dog_qry)
    else:
        if output_inline:
            output_to = {'inline':1}
        else:
            # Creates new collection
            output_to = {"replace":"user_count"}
        user_col = docs.map_reduce(mapper, reducer, output_to)
    return user_col

def print_user_stats(docs,dog_specific=None):
    """Gets the top contributing users in the database.
    If dog_specific input is specified (if not None),
    will perform a narrower search by filtering the map_reduce query
    by dog-related elements/regular-expression matches for certain dog fields.
    Map Reduce used to create a new collection, but if it already exists the
    mapreduce_users call will be skipped and the data will be read from
    the existing collection.
    If dog_specific input is specified, runs an additional query to
    print the names of all the entries created by the top-contributing user."""
    if dog_specific is not None:
        db_name = "dog_user_count"
    else:
        db_name = "user_count"
    if not check_collection_exists(db_name):
        print "Running MapReduce to generate new collection"
        users = mapreduce_users(docs,0,dog_specific)
    else:
        users = get_collection(db_name)
        
    total_users = users.find().count()
    top_usernames = users.find().sort("value",-1).limit(10)
    print "Top 10 contributing users (out of %d) in %s:"%(total_users,db_name)
    top_contributor = None
    for x,user in enumerate(top_usernames):
        if top_contributor is None:
            top_contributor = user['_id']
        print "%d) %s [%d]"%(x+1,user['_id'],user['value'])
    
    # Prints the count of all the keys the top contributing user has created
    mapreduce_print_user_created_key_count(docs,top_contributor)
    
    if dog_specific is not None:
        # Prints entry names that match the dog query from the top user
        dog_related_by_user(docs,top_contributor,5)
        
    
def test():
    docs = get_collection()
    total_size = size_of_collection(docs)
    if total_size is None:
        print "No documents exist in collection!"
    else:
        print "San Francisco has %d documents"%(total_size)
        lon = -122.39044189454
        lat = 37.776148988564
        create_indexes(docs)
        if 0:
            parks = find_parks(docs)
            for park in parks:
                pprint.pprint(park)
            print "Found %d parks"%(parks.count())
        pt = [lon,lat] # Change to None to skip location based part of search
        print "Searching for the closest 5 dog-related elements"
        nearby_dogs = dog_related(docs, pt, 5)
        if nearby_dogs:
            pprint.pprint(nearby_dogs)
            print "Aggregation Result (Dog related documents near Lat:%.2f, Lon:%.2f) Count: %d"%(lat,lon,len(nearby_dogs))
        else:
            print "No matches to address/loc aggregate pipeline"

        print_user_stats(docs,1)
        print_user_stats(docs)

if __name__ == '__main__':
    test()
