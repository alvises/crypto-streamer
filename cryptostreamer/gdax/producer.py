from .client import GdaxClient
from kafka import KafkaProducer
import json

class GdaxKafkaProducer(GdaxClient):

	def __init__(self,kafka_topic='gdax',gdax_kwargs={},kafka_kwargs={},matches_only=False):
		self._kafka_topic = kafka_topic
		self._matches_only = matches_only
		self._gdax_kwargs = gdax_kwargs
		self._kafka_kwargs = kafka_kwargs
		GdaxClient.__init__(self,**self._gdax_kwargs)

	# def on_(self):
	# 	self._kafka_producer = KafkaProducer(
	# 		bootstrap_servers="localhost:9092",
	# 		retries=5,
	# 		compression_type="gzip",
	# 		key_serializer=str.encode,
	# 		value_serializer=lambda v: json.dumps(v).encode('utf-8')
	# 	)

	def on_setup(self):
		self._kafka_producer = self._get_kafka_producer()


	def on_disconnected(self):
		self._kafka_producer.close()
		self._kafka_producer = None


	def on_message(self, msg):
		if self._matches_only: self._matches_only_on_message(msg)
		else: self._all_msg_on_message(msg)

	def _matches_only_on_message(self,msg):
		if msg.get('type') != 'match': return
		self._send_to_kafka(msg)

	def _all_msg_on_message(self,msg):
		if msg.get('type') == 'heartbeat': return
		self._send_to_kafka(msg)

	def _send_to_kafka(self,msg):
		msg = msg.copy()
		msg.pop('maker_order_id', None)
		msg.pop('taker_order_id',None)
		self._kafka_producer.send(self._kafka_topic,key=msg['product_id'],value=msg)


	def on_match(self,match_msg):
		self._send_to_kafka(match_msg)

	def _get_kafka_producer(self):
		return KafkaProducer(**self._kafka_kwargs)