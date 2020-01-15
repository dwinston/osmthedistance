from collections import defaultdict

from pymongo import InsertOne, MongoClient
from tqdm import tqdm


class NullTarget:
    """
    Performs no-op parsing of the XML source in order to return the number of tags.

    This is useful for a first pass of a large XML source in order to initialize a progress indicator for a second pass
    using a different target, one that performs more time-consuming per-tag processing.

    """
    def __init__(self, ntags_estimate=None):
        self.pbar = tqdm(total=ntags_estimate)
        self._ntags = 0

    def start(self, tag, attrs):
        self._ntags += 1
        self.pbar.update(1)

    def end(self, tag):
        pass

    def close(self):
        self.pbar.close()
        return self._ntags


class MongoTarget:
    def __init__(self, connection_uri="mongodb://localhost/admin", dbname="osm", ntags=None, insert_batch_size=10000):
        self.pbar = tqdm(total=ntags)
        self._client = MongoClient(connection_uri)
        self._client.drop_database(dbname)
        self.db = self._client[dbname]
        self._depth = 0
        self._doc = None
        self._coll = None
        self._requests = defaultdict(list)
        self._insert_batch_size = insert_batch_size
        print(f"Parsing to database '{dbname}' of MongoDB instance at {connection_uri}...")

    def start(self, tag, attrs):
        if self._depth == 1:
            self._doc = defaultdict(list)
            self._doc.update(dict(attrs))
            self._coll = self.db[tag]
        elif self._depth == 2:
            self._doc[tag].append(dict(attrs))
        self._depth += 1
        self.pbar.update(1)

    def end(self, tag):
        self._depth -= 1
        if self._depth == 1:
            self._requests[tag].append(InsertOne(dict(self._doc)))
            if sum(len(docs) for _, docs in self._requests.items()) == self._insert_batch_size:
                for collname, docs in self._requests.items():
                    self.db[collname].bulk_write(docs, ordered=False)
                    self._requests = defaultdict(list)
            self._doc, self._coll = None, None

    def close(self):
        for collname, docs in self._requests.items():
            self.db[collname].bulk_write(docs, ordered=False)
            self._requests = defaultdict(list)
        collection_names = self.db.list_collection_names()
        self._client.close()
        self.pbar.close()
        return collection_names
