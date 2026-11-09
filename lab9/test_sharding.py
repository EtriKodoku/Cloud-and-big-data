
import random
import pytest

from sharding import Sharding


# Test 1: Uniform distribution
def test_uniform_distribution():
    random.seed(42)

    storage = Sharding(
        ["shard-1", "shard-2", "shard-3", "shard-4", "shard-5"],
        virtual_nodes=150
    )

    for i in range(10000):
        key = f"key-{random.getrandbits(64)}"
        storage.put(key, i)

    distribution = storage.distribution()

    average = 10000 / len(distribution)

    # Each shard should stay within 25% of the average.
    for count in distribution.values():
        assert abs(count - average) / average < 0.25


# Test 2: Stability when adding a shard
def test_stability_when_adding_shard():
    storage = Sharding(
        ["shard-1", "shard-2", "shard-3", "shard-4", "shard-5"]
    )

    keys = [f"key-{i}" for i in range(10000)]

    old_mapping = {
        key: storage.ring.get_shard(key).name
        for key in keys
    }

    storage.add_shard("shard-6")

    new_mapping = {
        key: storage.ring.get_shard(key).name
        for key in keys
    }

    remapped = sum(
        old_mapping[key] != new_mapping[key]
        for key in keys
    )

    remapped_ratio = remapped / len(keys)

    # Adding a sixth shard should move significantly fewer
    # than all keys.
    assert remapped_ratio < 0.35


# Test 3: Stability when removing a shard
def test_stability_when_removing_shard():
    storage = Sharding(
        ["shard-1", "shard-2", "shard-3", "shard-4", "shard-5"]
    )

    keys = [f"key-{i}" for i in range(10000)]

    old_mapping = {
        key: storage.ring.get_shard(key).name
        for key in keys
    }

    storage.remove_shard("shard-3")

    new_mapping = {
        key: storage.ring.get_shard(key).name
        for key in keys
    }

    remapped = sum(
        old_mapping[key] != new_mapping[key]
        for key in keys
    )

    remapped_ratio = remapped / len(keys)

    assert remapped_ratio < 0.35


# Test 4: Correct data retrieval
def test_put_and_get():
    storage = Sharding(["shard-1", "shard-2", "shard-3"])

    storage.put("user-1", {"name": "Alice"})
    storage.put("user-2", {"name": "Bob"})
    storage.put("user-3", {"name": "Charlie"})

    assert storage.get("user-1") == {"name": "Alice"}
    assert storage.get("user-2") == {"name": "Bob"}
    assert storage.get("user-3") == {"name": "Charlie"}


# Test 5: Data remains available after adding a shard
def test_data_after_adding_shard():
    storage = Sharding(["shard-1", "shard-2", "shard-3"])

    for i in range(100):
        storage.put(f"key-{i}", f"value-{i}")

    storage.add_shard("shard-4")

    for i in range(100):
        assert storage.get(f"key-{i}") == f"value-{i}"


# Test 6: Data remains available after removing a shard
def test_data_after_removing_shard():
    storage = Sharding(["shard-1", "shard-2", "shard-3"])

    for i in range(100):
        storage.put(f"key-{i}", f"value-{i}")

    storage.remove_shard("shard-2")

    for i in range(100):
        assert storage.get(f"key-{i}") == f"value-{i}"


# Test 7: Cannot remove the last shard
def test_cannot_remove_last_shard():
    storage = Sharding(["shard-1"])

    with pytest.raises(ValueError):
        storage.remove_shard("shard-1")


# Test 8: Visualization of distribution
def test_print_distribution(capsys):
    storage = Sharding(["shard-1", "shard-2", "shard-3"])

    for i in range(30):
        storage.put(f"key-{i}", i)

    storage.print_distribution()

    output = capsys.readouterr().out

    assert "Shard distribution:" in output
    assert "shard-1" in output
    assert "shard-2" in output
    assert "shard-3" in output
