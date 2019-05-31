from kafka.client import KafkaClient
from kafka.cluster import ClusterMetadata

client = KafkaClient(bootstrap_servers='localhost:9092', client_id='test_store_builder')
response_metadata = client.poll(future=client.cluster.request_update())
cluster_metadata = ClusterMetadata(bootstrap_servers='localhost:9092')
cluster_metadata.update_metadata(response_metadata[0])
cluster_metadata.partitions_for_topic('test-assignor')
