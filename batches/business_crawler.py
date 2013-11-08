import sys
import os
from pymongo import MongoClient

from yelpy.yelpy_client import YelpyClient

class BusinessCrawler(object):

    yc = YelpyClient(max_calls=7000)

    def __init__(self):
        super(BusinessCrawler, self).__init__()
        mongo_url = os.getenv('MONGOHQ_URL')
        assert mongo_url
        self.client = MongoClient(mongo_url)
        self.db = self.client.get_default_database()

    def run(self):
        while True:
            search_request = self.db.YELP_SEARCH.find_one({'complete': {'$exists': False}})
            if search_request is None:
                return
            print search_request
            city = search_request['city']
            state = search_request['state']
            try:
                self.crawl(city, state, False, 7000)
            except Exception as e:
                print e
                sys.exit(0)
            search_request.update({'complete': True})
            self.db.YELP_SEARCH.save(search_request)

    def crawl(self, city, state, deals_filter, total):
        offset = 0
        print offset
        while offset < total:
            result = self.search(city, state, offset, deals_filter)
            business_count = len(result['businesses'])
            total_possible = result['total']
            if total_possible < total:
                raise Exception
            if business_count <= 0:
                return
            for biz in result.get('businesses', []):
                if self.business_exists(biz):
                    offset += 1
                    continue
                else:
                    assert biz['id']
                    self.add_business(biz['id'])
                    offset += 1                

    def search(self, city, state, offset, deals_filter=False):
        location = "%s, %s" % (city, state,)
        results = self.yc.search(location=location, offset=offset, deals_filter=deals_filter)
        return results
        

    def reset_search(self, state):
        pass

    def add_business(self, business_id):
        business = self.yc.business(business_id)
        assert business['id']
        if not self.business_exists(business['id']):
            business.update({'yelp_supplement': True, 'business_id': business['id']})
            self.db.YELP_BUSINESSES.insert(business)
            reviews = business.get('reviews', [])
            if reviews:
                for r in reviews:
                    r.update({'yelp_supplement': True, 'business_id': business['id']})
                self.db.YELP_REVIEWS.insert(reviews)

    def business_exists(self, business_id):
        if self.db.YELP_BUSINESSES.find({"id": business_id}).count() >= 1:
            return True
        else:
            return False


if __name__ == "__main__":
    BusinessCrawler().run()
