"""
Imports the json dump data into mongoDB.  Expects data.py to have generated
the json dump of the shaped and cleaned *.osm data.

Start mongoDB server locally
mongod --dbpath /Users/cminnich/data/db/

Work within the shell
>>> import mongo_audit as ma
>>> ma.test()
>>> docs = ma.get_collection()
>>> ma.find_name("Dog Park", docs, limit_results=5, printout=1):


San Francisco has 3218578 documents
Searching for the closest 5 dog-related elements
[{u'_id': ObjectId('53df424b303db22d668e7359'),
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
 {u'_id': ObjectId('53df424c303db22d668e73e0'),
  u'created': {u'changeset': u'783501',
               u'timestamp': u'2009-03-11T06:34:06Z',
               u'uid': u'4732',
               u'user': u'iandees',
               u'version': u'1'},
  u'distance': 0.022914830475510236,
  u'ele': u'1',
  u'gnis': {u'county_id': u'075',
            u'created': u'01/01/1995',
            u'feature_id': u'1656929',
            u'state_id': u'06'},
  u'id': u'358804967',
  u'leisure': u'park',
  u'name': u'Warm Water Cove Park',
  u'pos': [-122.3833038, 37.7543743],
  u'type': u'node'},
 {u'_id': ObjectId('53df424b303db22d668e7376'),
  u'created': {u'changeset': u'783501',
               u'timestamp': u'2009-03-11T06:31:46Z',
               u'uid': u'4732',
               u'user': u'iandees',
               u'version': u'1'},
  u'distance': 0.024245846848382813,
  u'ele': u'7',
  u'gnis': {u'county_id': u'075',
            u'created': u'11/01/1994',
            u'feature_id': u'1655668',
            u'state_id': u'06'},
  u'id': u'358803650',
  u'leisure': u'park',
  u'name': u'Jackson Square Historic District',
  u'pos': [-122.4030265, 37.7968731],
  u'type': u'node'},
 {u'_id': ObjectId('53df429b303db22d66906c27'),
  u'address': {u'street': u'Post Street'},
  u'created': {u'changeset': u'3734702',
               u'timestamp': u'2010-01-28T11:00:09Z',
               u'uid': u'116029',
               u'user': u'Gregory Arenius',
               u'version': u'1'},
  u'distance': 0.02448346957080776,
  u'id': u'621793520',
  u'name': u'Checkob Pet Botique',
  u'pos': [-122.4120149, 37.7877268],
  u'shop': u'pet',
  u'source': u'survey',
  u'type': u'node'},
 {u'_id': ObjectId('53df424b303db22d668e7388'),
  u'created': {u'changeset': u'783501',
               u'timestamp': u'2009-03-11T06:31:58Z',
               u'uid': u'4732',
               u'user': u'iandees',
               u'version': u'1'},
  u'distance': 0.026305380005461003,
  u'ele': u'6',
  u'gnis': {u'county_id': u'075',
            u'created': u'11/01/1994',
            u'feature_id': u'1655716',
            u'state_id': u'06'},
  u'id': u'358803741',
  u'leisure': u'park',
  u'name': u'Northeast Waterfront Historic District',
  u'pos': [-122.4010821, 37.8002064],
  u'type': u'node'}]
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
Showing all fields iandees has entered and the associated count
  amenity : 543
  area : 1
  created : 972
  ele : 970
  gnis : 970
  highway : 1
  id : 972
  landuse : 17
  leisure : 270
  man_made : 17
  name : 970
  natural : 61
  node_refs : 1
  place : 10
  pos : 971
  type : 972
  waterway : 53
iandees created 972 total documents
iandees created 254 dog-related entries with the following names (only displaying 5)
 Agate Beach County Park
 Alvarado Park
 Bayview Playground
 Beach State Park
 Bolinas Quail Refuge
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
Showing all fields ediyes has entered and the associated count
  access : 11
  address : 463
  amenity : 177
  area : 18
  area_m2 : 21
  atm : 1
  baby_hatch : 6
  barrier : 3
  bicycle : 1
  bldgid : 21
  building : 62778
  bulb : 1
  capacity : 3
  construction : 5
  contact : 1
  craft : 1
  created : 731437
  cuisine : 26
  cycleway : 1
  denomination : 47
  designation : 1
  dispensing : 2
  drive_through : 1
  ele : 117
  electrified : 1
  emergency : 4
  entrance : 2
  fax : 5
  fee : 3
  fixme : 1
  foot : 2
  footway : 4
  frequency : 1
  furniture : 1
  gauge : 1
  gnis : 117
  grade : 6
  grades : 4
  height : 1
  highway : 60
  historic : 1
  horse : 1
  id : 731437
  import_uuid : 1
  incline : 1
  internet_access : 3
  is_in : 2
  landuse : 12
  lanes : 7
  layer : 11
  lcn_ref : 1
  leisure : 9
  maxspeed : 2
  name : 288
  node_refs : 63862
  note : 10
  old_amenity : 2
  old_denomination : 2
  old_religion : 2
  oneway : 1
  opening_hours : 8
  operator : 27
  other_names : 11
  parking : 5
  payment : 3
  phone : 29
  place : 1
  pos : 667575
  railway : 3
  ref : 7
  religion : 62
  route_ref : 1
  service : 12
  shelter : 1
  shop : 26
  smoking : 1
  social_facility : 2
  source : 344
  sport : 2
  tactile_paving : 1
  ticker : 1
  tiger : 19
  tourism : 4
  trolley_wire : 1
  tunnel : 2
  type : 731437
  url : 7
  verified : 5
  voltage : 1
  website : 19
  wheelchair : 9
  wifi : 3
  wikipedia : 2
ediyes created 731437 total documents
"""
import pymongo
import pprint
import re
from audit import dog_re, dog_include_keys, not_dog_amenities

#OSMFILE = "example.osm"
#OSMFILE = "example_sf.osm"
OSMFILE = "san-francisco.osm"

any_char_re = re.compile(r".*\w+.*")
dog_re = re.compile(r"(dog(?!patch))|(\bpup)|(\bpet(s|\b|\'))|(animal)|(canine)|(k9)|(\bvet(erinary|\b))", re.IGNORECASE)
not_dog_amenities_re = re.compile(r"^((fast_food)|(pub)|(restaurant)|(cafe)|(parking))$", re.IGNORECASE)
not_dog_names_re = re.compile(r"^.*rec(reation)?\s*ce?nte?r.*$", re.IGNORECASE)
dog_park_re = re.compile(r".*((park)|(dog)).*", re.IGNORECASE)

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
    docs.create_index([("pos", pymongo.GEO2D)])

def check_before_create_indexes(docs):
    ind = docs.index_information()
    if 'pos_2d' not in ind.keys():
        print "Creating GEO2D Index on 'pos' keys"
        create_indexes(docs)
    
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

def find_name(srch_str, docs, limit_results=5, printout=None):
    srch_str = '.*'+re.sub(r'\W','.*',srch_str)+'.*'
    print 'Regex: %s'%(srch_str)
    custom_re = re.compile(srch_str,re.IGNORECASE)
    query = { "$and" : [{ "pos" :  { "$exists" : 1 } },
                       { "name" : custom_re }] }
    result_limit = 5
    results = docs.find(query).limit(limit_results)
    if  printout is not None:
        print 'Top %d Results: %s Search'%(limit_results,srch_str)
        for r in results:
            if r['type'] == 'way' and 'node_refs' in r.keys():
                mapped_node_refs = []
                for nr in r['node_refs']:
                    nr_query = { "_id" : nr }
                    found_nr = docs.find_one(nr_query)
                    mapped_node_refs.append(found_nr)
                r['node_refs'] = mapped_node_refs
            pprint.pprint(r)
    return results

def dog_related_by_user(docs, username, print_names=20):
    """print_names specifies the maximum number of names to print to the screen"""
    user_and_dog_query = {"$and":[{ "created.user" : username }, dog_qry]}
    all_posts = docs.find(user_and_dog_query)
    if print_names is not None:
        print '%s created %d dog-related entries with the following names (only displaying %d)'%(username,all_posts.count(),print_names)
        for i in all_posts:
            if 'name' in i.keys() and print_names > 0:
                print ' %s'%(i['name'])
                print_names -= 1
    return all_posts
                
def mapreduce_print_user_created_key_count(docs, username):
    """Runs MapReduce to generate a count of all the keys (fields)
    created by this user, then prints out each key and count"""
    from bson.code import Code
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
    print "Showing all fields %s has entered and the associated count"%(username)
    total_entries = 0
    for keys in user_dict['results']:
        if keys['_id'] == '_id':
            total_entries = int(keys['value'])
        else:
            print "  %s : %d"%(keys['_id'],int(keys['value']))
    print "%s created %d total documents"%(username,total_entries)
    
    
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
        check_before_create_indexes(docs)
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