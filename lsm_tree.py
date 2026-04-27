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
    
    def is_full(self):
        return len(self.data) >= len(self.max_size)

    def flush_to_sstable(self, path):
        with open(path, "w") a f:
            for k, v in self.data.item():
                f.write(json.dumps({"k": k, "v": v}) + "\n")
        self.data.clear()



class LSMDM:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.memtable = MemTable()
        self.sstables = [] # newest last, search newest first
        self.wal = open(os.path.join(data_dir, "wal.log"), "a") # write ahead log