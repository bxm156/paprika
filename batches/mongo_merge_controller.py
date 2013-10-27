import os
import sys
import logging
from pymongo import MongoClient
from yelpy.yelpy_client import YelpyClient


from business_resolver import BusinessResolver

class MongoMergeController(object):

    client = None
    db = None
    business_resolver = BusinessResolver()
    yc = YelpyClient(max_calls=7500)

    def __init__(self):
        super(MongoMergeController, self).__init__()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def connect(self):
        mongo_url = os.getenv('MONGOHQ_URL')
        assert mongo_url
        self.client = MongoClient(mongo_url)
        return self.client != None

    def disconnect(self):
        self.client.disconnect()

    def get_db(self):
        return self.client.get_default_database()

    def process(self, input_values):
        self.connect()
        db = self.get_db()
        for input_value in input_values:
            self.logger.info("Attempting.. %s" % (input_value.get('business_id', '?')))
            self.merge(db, input_value, self.yc)
        self.logger.info("Total Queries: %s" % (self.yc.total_calls))
        self.disconnect()

    def find_business(self, name, city, state, phone, yelpy_client=None):
        yelpy_client = yelpy_client or self.yc
        location = city + ", " + state
        result = yelpy_client.search(term=name, location=location, limit=5)
        region = result['region']
        total = result['total']
        businesses = result['businesses']
        if not businesses:
            return None
        return self.business_resolver.resolve(businesses, { 'name': name, 'location_city': city, 'location_state_code': state, 'phone': phone})

    def merge(self, db, input_json, yc):
        assert input_json['_id']
        assert input_json['name']
        assert input_json['city']
        assert input_json['state']
        business = self.find_business(input_json['name'], input_json['city'], input_json['state'], phone=None, yelpy_client=yc)
        if business is None:
            return
        input_json.update(business)
        business = yc.business(business['id'])
        reviews  = business.get('reviews', [])
        for r in reviews:
            r.update({'business_id': input_json['business_id']})
            r.update({'yelp_supplement': True})

        input_json.update(business)
        input_json.update({'yelp_supplement': True})
        db.YELP_BUSINESSES.save(input_json)
        db.YELP_REVIEWS.insert(reviews)

    def process_rand(self):
        self.connect()
        db = self.get_db()
        input_value = db.YELP_BUSINESSES.find_one()
        input_values = [input_value]
        self.disconnect()
        self.process(input_values)
