
import hashlib
from bisect import bisect_left


class ShardNode:
    """In-memory storage node."""

    def __init__(self, name: str):
        self.name = name
        self.data = {}

    def put(self, key, value):
        self.data[key] = value

    def get(self, key):
        if key not in self.data:
            raise KeyError(f"Key {key!r} not found in shard {self.name}")
        return self.data[key]


class ConsistentHashRing:
    """Consistent hashing ring with virtual nodes."""

    def __init__(self, virtual_nodes: int = 150):
        if virtual_nodes < 1:
            raise ValueError("virtual_nodes must be at least 1")

        self.virtual_nodes = virtual_nodes
        self.ring = []
        self.shards = {}

    @staticmethod
    def _hash(value: str) -> int:
        return int(
            hashlib.sha256(value.encode("utf-8")).hexdigest(),
            16
        )

    def add_shard(self, shard: ShardNode):
        if shard.name in self.shards:
            raise ValueError(f"Shard {shard.name} already exists")

        self.shards[shard.name] = shard

        for i in range(self.virtual_nodes):
            node_key = f"{shard.name}:{i}"
            self.ring.append((self._hash(node_key), shard.name))

        self.ring.sort()

    def remove_shard(self, shard_name: str):
        if shard_name not in self.shards:
            raise ValueError(f"Shard {shard_name} does not exist")

        if len(self.shards) == 1:
            raise ValueError("Cannot remove the last shard")

        del self.shards[shard_name]

        self.ring = [
            item for item in self.ring
            if item[1] != shard_name
        ]

    def get_shard(self, key: str) -> ShardNode:
        if not self.ring:
            raise RuntimeError("No shards available")

        key_hash = self._hash(str(key))
        index = bisect_left(self.ring, (key_hash, ""))

        if index == len(self.ring):
            index = 0

        shard_name = self.ring[index][1]
        return self.shards[shard_name]


class Sharding:
    """Sharded in-memory key-value storage."""

    def __init__(self, shard_names, virtual_nodes: int = 150):
        if not shard_names:
            raise ValueError("At least one shard is required")

        if len(set(shard_names)) != len(shard_names):
            raise ValueError("Shard names must be unique")

        self.ring = ConsistentHashRing(virtual_nodes)

        for name in shard_names:
            self.ring.add_shard(ShardNode(name))

    def put(self, key: str, value):
        shard = self.ring.get_shard(key)
        shard.put(key, value)

    def get(self, key: str):
        shard = self.ring.get_shard(key)
        return shard.get(key)

    def add_shard(self, shard_name: str):
        """Add a shard and redistribute existing data."""
        existing_data = self._collect_data()

        self.ring.add_shard(ShardNode(shard_name))
        self._redistribute(existing_data)

    def remove_shard(self, shard_name: str):
        """Remove a shard and redistribute its data."""
        existing_data = self._collect_data()

        self.ring.remove_shard(shard_name)
        self._redistribute(existing_data)

    def _collect_data(self):
        data = {}

        for shard in self.ring.shards.values():
            data.update(shard.data)

        return data

    def _redistribute(self, data):
        # Clear all shard storage before redistribution.
        for shard in self.ring.shards.values():
            shard.data.clear()

        for key, value in data.items():
            shard = self.ring.get_shard(key)
            shard.put(key, value)

    def distribution(self):
        """Return the number of keys stored in each shard."""
        return {
            name: len(shard.data)
            for name, shard in self.ring.shards.items()
        }

    def print_distribution(self):
        """Print the current key distribution."""
        print("Shard distribution:")

        for name, count in self.distribution().items():
            print(f"{name}: {count} keys")
