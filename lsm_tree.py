from sortedcontainers import SortedDict
import json, os

class MemTable:
    def __init__(self, max_size=1000):
        self.data = SortedDict()
        self.max_size = max_size

    def put(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)
    
    