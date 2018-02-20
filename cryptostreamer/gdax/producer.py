import json
from .client import GdaxClient
from kafka import KafkaProducer

from cryptostreamer import get_logger
LOGGER = get_logger('GdaxKafkaProducer')

KAFKA_RES_TIMEOUT = 30


class GdaxKafkaProducer(GdaxClient):

	@classmethod
	def create_with_environment(cls):
		kafka_topic = cls.get_str_from_env('CRYPTO_KAFKA_TOPIC')
		matches_only = cls.get_boolean_from_env('CRYPTO_KAFKA_MATCHES_ONLY')
		gdax_kwargs = GdaxClient.kwargs_from_environment()
		kafka_kwargs = GdaxKafkaProducer.kwargs_from_environment()
		return cls(kafka_topic,gdax_kwargs,kafka_kwargs,matches_only)

	@classmethod
	def kwargs_from_environment(cls):
		kwargs= {
			'bootstrap_servers': cls.get_list_from_env('CRYPTO_KAFKA_BOOTSTRAP_SERVERS')
		}
		return {k: v for k,v in kwargs.items() if v is not None}


	def __init__(self,kafka_topic='gdax',gdax_kwargs={},kafka_kwargs={},matches_only=False):
		self._kafka_topic = kafka_topic
		self._matches_only = matches_only
		self._gdax_kwargs = gdax_kwargs
		self._kafka_kwargs = kafka_kwargs
		GdaxClient.__init__(self,**self._gdax_kwargs)


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
		if msg.get('type') in ['heartbeat','subscriptions']: return
		self._send_to_kafka(msg)

	def _send_to_kafka(self,msg):
		msg = msg.copy()
		msg.pop('maker_order_id', None)
		msg.pop('taker_order_id',None)
		try:
			future = self._kafka_producer.send(
				self._kafka_topic,
				key=msg['product_id'],value=msg)
			future.get(timeout=5)
		except Exception as e:
			self.on_error(e)


	def on_error(self,e):
		self.stop()
		raise e

	def _get_kafka_producer(self):
		kwargs = self._kafka_kwargs.copy()
		kwargs['key_serializer'] = str.encode
		kwargs['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
		kwargs['compression_type'] = 'gzip'
		return KafkaProducer(**kwargs)