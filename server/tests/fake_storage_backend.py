from copy import deepcopy
import hashlib
import json

from search_backend import BackendError
from storage_backend import validate_envelope


class FakeStorage:
    def __init__(self):
        self.state = None
        self.saves = 0
        self.backups = {}
        self.loaded_raw = None

    def load(self):
        if self.state is not None and self.state.get("format") == 1:
            self.loaded_raw = json.dumps(self.state, separators=(",", ":"), ensure_ascii=False).encode()
        return deepcopy(self.state)

    def backup_format1(self):
        # Match the real backend's create-only, exact-original-byte contract.
        from server import PARTITION
        digest = hashlib.sha256(self.loaded_raw).hexdigest()
        name = hashlib.sha256(PARTITION.encode()).hexdigest() + "." + digest + ".format1.json"
        self.backups.setdefault(name, self.loaded_raw)
        if hashlib.sha256(self.backups[name]).hexdigest() != digest:
            raise BackendError(503, "Format1 backup digest mismatch")

    def save(self, state):
        validate_envelope(state)
        self.state = deepcopy(state)
        self.saves += 1
