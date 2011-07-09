import urllib2
import urllib
import json
import pymysql
import pymongo
import sys
import traceback
import time

def begin_processing(url,db):
    dataCount = 0
    json_dict = None
    try:
        req = urllib2.urlopen(url)
        for data in req:
            dataCount+=1
            json_dict = json.loads(data)
            if "limit" in json_dict:
                print "reached rate limit"
                continue
            db.save(json_dict)
            if dataCount % 100 == 0:
                print "inserted %d results" % dataCount
#TODO handle IncompleteRead error
    except Exception, e:
        print "error happened:"
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60
        print json.dumps(json_dict, sort_keys=True, indent=4)
        return False

class MongoConnection:
    def __init__(self, dbname,collection):
        self.connection = pymongo.Connection(host='127.0.0.1')
        self.database = self.connection[dbname]
        self.collection = self.database[collection]

    def save(self, dict):
        self.collection.save(dict)

class MysqlConnection:
    def __init__(self, dbname, host='localhost', user='root', password='root'):
        self.connection = pymysql.connect(host, user, password, dbname,charset='utf8')
        self.cursor = self.connection.cursor()

    def save(self, tweet_dict):
        #right now we'll just convert and commit immediately
        for sql in self.convert_tweet(tweet_dict):
            if sql == None:
                continue
            self.cursor.execute(sql[0],sql[1])

    def convert_tweet(self, tweet):
        if 'user' not in tweet:
            print tweet
            print json.dumps(tweet)

        user_sql = self._convert_user_dict(tweet['user'])
        place_sql = self._convert_place_dict(tweet['place'])
        tweet_sql = self._convert_tweet_dict(tweet)
        return (user_sql,place_sql,tweet_sql) 


    def _convert_user_dict(self, user_dict):
        columns = ('id','created','description','followers_count','following_count','name','screen_name','statuses_count','utc_offset','verified')
        value_props = ('id','created_at','description','followers_count','friends_count','name','screen_name','statuses_count','utc_offset','verified')
        values = map(lambda x: user_dict[x], value_props)
        values[1] = self._parse_timestamp(values[1])
        query = 'INSERT IGNORE INTO users %s VALUES %s' % (self._convert_to_string(columns),self._create_value_string(values))
        return (query,values)
        
    def _create_value_string(self, values):
        count = len(values)
        if count is 0:
            return None
        string = '('
        for val in values:
            string += '%s,'

        return string[0:-1] + ')'
            
    def _convert_to_string(self, iterable):
        column_string = '('
        for c in iterable:#TODO rename
            column_string += str(c) + ','
        return column_string[0:-1] + ')'

    def _convert_place_dict(self, place_dict):
        return None
#TODO implement
        columns = ('id','coord')
        values = [1,'Point(1 1)']
        query = "INSERT IGNORE INTO places (id,coord) VALUES (%s, GeomFromText(%s))"
        return (query,values)

    def _convert_tweet_dict(self, tweet_dict):
        columns = ('id','created','reply_to_screen_name','reply_to_id','retweet_count','text','truncated','geo','geo_type','reply_to_user_id','user_id','place_id')
        
        values = [tweet_dict['id'],self._parse_timestamp(tweet_dict['created_at']),tweet_dict['in_reply_to_screen_name'],tweet_dict['in_reply_to_status_id'],tweet_dict['retweet_count'],tweet_dict['text'],tweet_dict['truncated']]
        if tweet_dict['geo'] != None and "coordinates" in tweet_dict['geo']:
            if tweet_dict['geo']['type'] != "Point":
                print "type was %s" % tweet_dict['geo']['type']

            values.append("POINT(%s %s)" % (tweet_dict['geo']['coordinates'][0],tweet_dict['geo']['coordinates'][1]))
            values.append(tweet_dict['geo']['type'])
        else:
            values.append("POINT(-90 -90)")
            values.append("Point")


        values.append(tweet_dict['in_reply_to_user_id'])
        values.append(tweet_dict['user']['id'])
        values.append(None)


        query = 'INSERT IGNORE INTO tweets ' + self._convert_to_string(columns) + ' VALUES (%s,%s,%s,%s,%s,%s,%s,GeomFromText(%s),%s,%s,%s,%s)'
        return (query,values)

    def _parse_timestamp(self,dtime):
        return time.strptime(dtime, '%a %b %d %H:%M:%S +0000 %Y')
                                        
if __name__ == "__main__":
    arglen = len(sys.argv)
    if arglen < 3:
        print "%s <url> <mysql|mongo>" % sys.argv[0]
        sys.exit(1)

    if sys.argv[2] == 'mongo':
        db = MongoConnection('geo','tweets')
    elif sys.argv[2] == 'mysql':
        db = MysqlConnection('geotweets')
    else:
        print "invalid database type"
        sys.exit(1)
    begin_processing(sys.argv[1],db)
