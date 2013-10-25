from collections import defaultdict
from collections import MutableMapping

import jaro

class BusinessResolver(object):

    JARO_THRESHOLD = 0.75

    def map_to_scores(self, input_dicts, matching_dict):
        mapping = defaultdict(list)
        for input_dict in input_dicts:
            score = self.score(input_dict, matching_dict)
            mapping[score].append(input_dict)
        return mapping

    def resolve(self, input_dicts, matching_dicts):
        mapping = self.map_to_scores(input_dicts, matching_dicts)
        highest_score = max(k for k, v in mapping.iteritems() if v != 0)
        highest_businesses = mapping[highest_score]
        if len(highest_businesses) == 1:
            return highest_businesses[0]
        else:
            return self.handle_ties(highest_businesses)

    def handle_ties(self, businesses):
        return businesses[0]

    def flatten(self, d, parent_key=''):
        #http://stackoverflow.com/questions/6027558/flatten-nested-python-dictionaries-compressing-keys
        items = []
        for k, v in d.items():
            new_key = parent_key + '_' + k if parent_key else k
            if isinstance(v, MutableMapping):
                items.extend(self.flatten(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def score(self, input_dict, matching_dict):
        score = 0
        flat_input = self.flatten(input_dict)
        for key, value in matching_dict.items():
            if matching_dict[key] != None and key in flat_input:
                if jaro.jaro_winkler_metric(unicode(flat_input[key]), unicode(matching_dict[key])) > self.JARO_THRESHOLD:
                    score += 1
                else: score -= 1
        return score
