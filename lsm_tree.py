from sortedcontainers import SortedDict
import json, os
from typing import Protocol

class Comparable(Protocol):
    def __lt__(self, other: object, /) -> bool: ...


class MemTable[K: Comparable, V]:
    def __init__(self, max_size: int=10) -> None:
        self.data = SortedDict()
        self.max_size = max_size

    def put(self, key: K, value: V) -> None:
        self.data[key] = value

    def get(self, key: K) -> V | None:
        return self.data.get(key)
    
    def is_full(self) -> bool:
        return len(self.data) >= self.max_size

    def flush_to_sstable(self, path: str) -> None:
        with open(path, "w") as f:
            for k, v in self.data.items():
                f.write(json.dumps({"k": k, "v": v}) + "\n")
        self.data.clear()



class LSMDB[K: Comparable, V]:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.memtable = MemTable()
        self.sstables = [] # newest last, search newest first
        self.wal = open(os.path.join(data_dir, "wal.log"), "a") # write ahead log
    
    def put(self, key: K, value: V) -> None:
        self.wal.write(json.dumps({"k": key, "v": value}) + "\n")
        self.wal.flush()
        os.fsync(self.wal.fileno())
        self.memtable.put(key, value)
        if self.memtable.is_full():
            self._flush()

    def get(self, key: K) -> V | None:
        v = self.memtable.get(key)
        if v is not None:
            return v
        
        for path in reversed(self.sstables):
            v = self._search_sstables(path, key)
            if v is not None:
                return v
        return None
    
    def _flush(self) -> None:
        path = os.path.join(self.data_dir, f"sst_{len(self.sstables)}.json")
        self.memtable.flush_to_sstable(path)
        self.sstables.append(path)

    def _search_sstables(self, path: str, key: K) -> V | None:
        with open(path) as f:
            for line in f:
                entry = json.loads(line)
                if entry["k"] == key:
                    return entry["v"]
        return None
