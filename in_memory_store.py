# in-memory store for saving the values

import threading

class in_memory_store:
    def __init__(self):
        self._store = {}
        
    def put(self, key, value):
        with threading.Lock():
            self._store[key] = value
        
    def get(self, key):
        with threading.Lock():
            if key in self._store:
                return self._store[key]
            else:
                return None
        
    def delete(self, key):
        with threading.Lock():
            if key in self._store:
                del self._store[key]
            
            