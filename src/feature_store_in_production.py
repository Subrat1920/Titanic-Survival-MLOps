## although this template is not being used in any pipeline, this is just the recommended redis caching in production...
## enabling real time caching, with ttl (Time to Live)
import redis

class RedisFeatureStore:
    def __init__(self, host="localhost", port=6379, db=0):
        # StrictRedis is deprecated — use Redis()
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )

    # -----------------------------
    #  SINGLE ENTITY OPS
    # -----------------------------
    def store_features(self, entity_id, features: dict):
        """
        Store features using a HASH instead of JSON.
        Each key becomes a field, each value becomes a string.
        """
        key = f"entity:{entity_id}:features"
        self.client.hset(key, mapping=features)

    def get_features(self, entity_id):
        """
        Get all features from HASH.
        Returns {} if key doesn't exist.
        """
        key = f"entity:{entity_id}:features"
        data = self.client.hgetall(key)
        return data if data else None

    # -----------------------------
    #  BATCH OPS
    # -----------------------------
    def store_batch_features(self, batch_data: dict):
        """
        Store many entity feature dicts using a pipeline.
        Avoids round trip per key – much faster.
        """
        pipe = self.client.pipeline()
        for entity_id, features in batch_data.items():
            key = f"entity:{entity_id}:features"
            pipe.hset(key, mapping=features)
        pipe.execute()

    def get_batch_features(self, entity_ids):
        """
        Get multiple entities’ features with pipelining.
        """
        pipe = self.client.pipeline()
        keys = [f"entity:{eid}:features" for eid in entity_ids]

        for key in keys:
            pipe.hgetall(key)

        results = pipe.execute()

        # Convert list output back into a dict {id: features}
        return {
            entity_id: (result if result else None)
            for entity_id, result in zip(entity_ids, results)
        }

    # -----------------------------
    #  LIST ALL ENTITY IDS
    # -----------------------------
    def get_all_entity_ids(self):
        """
        NEVER use KEYS in production.
        SCAN is the correct way.
        Returns a generator of entity IDs.
        """
        pattern = "entity:*:features"
        for key in self.client.scan_iter(match=pattern):
            # key format → entity:<id>:features
            yield key.split(":")[1]

    # -----------------------------
    #  OPTIONAL: TTL SUPPORT
    # -----------------------------
    def store_features_with_ttl(self, entity_id, features, ttl_seconds):
        """
        Store features and auto-expire after TTL.
        Useful for real-time ML predictions.
        """
        key = f"entity:{entity_id}:features"
        pipe = self.client.pipeline()
        pipe.hset(key, mapping=features)
        pipe.expire(key, ttl_seconds)
        pipe.execute()

