import os
import time
import random
import logging

from pymongo import MongoClient
from mongo_merge_controller import MongoMergeController


class YelpMergeBatch(object):

    min_sleep = 110
    max_sleep = 180

    max_iterations = 2200
    iterations = 0
    
    def __init__(self):
        super(YelpMergeBatch, self).__init__()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        random.seed()
        self.db = self.connect().get_default_database()

    def connect(self):
        mongo_url = os.getenv('MONGOHQ_URL')
        assert mongo_url
        return MongoClient(mongo_url)

    def run(self):
        merger = MongoMergeController()
        while True:
            self.iterations += 1
            if self.iterations > self.max_iterations:
                return

            #results = self.db.YELP_BUSINESSES.find({"yelp_supplement": True}).limit(5)
            results = self.db.YELP_BUSINESSES.find({"yelp_supplement": {"$exists": False}}).limit(5)
            blobs = list(results)

            merger.process(blobs) 


            sleep_time = random.randint(self.min_sleep, self.max_sleep)
            self.logger.info("Sleeping for %s seconds..." % (sleep_time,))
            time.sleep(sleep_time)

        print "Run complete"




if __name__ == "__main__":
    YelpMergeBatch().run()
