import pytest
from cryptostreamer.gdax import GdaxKafkaProducer
from mock import MagicMock
import os

class TestKafkaProducer:
	def unset_envvars(self):
		os.unsetenv('CRYPTO_GDAX_PRODUCTS')
		os.unsetenv('CRYPTO_GDAX_CHANNELS')
		os.unsetenv('CRYPTO_GDAX_TIMEOUT')
		os.unsetenv('CRYPTO_KAFKA_BOOTSTRAP_SERVERS')
		os.unsetenv('CRYPTO_KAFKA_TOPIC')
		os.unsetenv('CRYPTO_KAFKA_MATCHES_ONLY')


	def setUp(self): self.unset_envvars()
	def tearDown(self): self.unset_envvars()


	def test__start__it_tries_to_connect_to_kafka_before_connecting_to_gdax(self):
		gdax_producer = GdaxKafkaProducer("gdax",{'products':['BTC-USD']})
		gdax_producer._connect= MagicMock()
		gdax_producer._subscribe = MagicMock()
		gdax_producer._mainloop = MagicMock()

		kafka_producer_mock = MagicMock()
		def _get_kafka_producer():
			gdax_producer._connect.assert_not_called()
			return kafka_producer_mock

		gdax_producer._get_kafka_producer = _get_kafka_producer

		gdax_producer.start()

		assert gdax_producer._kafka_producer is not None


	def test__disconnect__disconnects_from_gdax_and_kafka(self):
		gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']})
		ws_mock = MagicMock()
		kp_mock = MagicMock()
		gdax_producer._ws = ws_mock
		gdax_producer._kafka_producer = kp_mock

		gdax_producer._disconnect()

		ws_mock.close.assert_called_once()
		kp_mock.close.assert_called_once()


	def test_if__matches_only__option_is_True_then_only_match_msg_are_published_to_kafka(self):
		gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{},matches_only=True)
		gdax_producer._kafka_producer = MagicMock()

		gdax_producer.on_message({'type': 'last_match', 'product_id': 'BTC-USD'})
		gdax_producer._kafka_producer.send.assert_not_called()

		gdax_producer.on_message({'type': 'match', 'product_id': 'BTC-USD'})
		gdax_producer._kafka_producer.send.assert_called_once()


	def test_hearbeat_messages_are_never_published_to_kafka(self):
		gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{})
		gdax_producer._kafka_producer = MagicMock()
		gdax_producer.on_message({'type': 'heartbeat'})
		gdax_producer._kafka_producer.send.assert_not_called()

		gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{},matches_only=True)
		gdax_producer._kafka_producer = MagicMock()
		gdax_producer.on_message({'type': 'heartbeat'})
		gdax_producer._kafka_producer.send.assert_not_called()


	def test_subscriptions_messages_are_never_published_to_kafka(self):
		gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{})
		gdax_producer._kafka_producer = MagicMock()
		gdax_producer.on_message({'type': 'subscriptions'})
		gdax_producer._kafka_producer.send.assert_not_called()


	def test_when_sending_to_kafka_the_key_is_always_the_product_id(self):
		# the reason is because having product id as key enforce the order of the received trades
		# all the messages of one product will go in just one partition

		gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{})
		gdax_producer._kafka_producer = MagicMock()

		msg = {'type': 'match', 'product_id': 'BTC-USD'}
		gdax_producer.on_message(msg)
		gdax_producer._kafka_producer.send.assert_called_once_with('gdax',value=msg,key='BTC-USD')


	def test_order_id_are_filtered(self):
		gdax_producer = GdaxKafkaProducer("gdax",{'products': ['LTC-EUR']},{})
		gdax_producer._kafka_producer = MagicMock()
		full_msg = {
	        "type": "match", "trade_id": 12345678,
	        "maker_order_id": "12345678",
	        "taker_order_id": "12345678",
	        "side": "sell", "size": "3.53526947",
	        "price": "183.80000000", "product_id": "LTC-EUR",
	        "sequence": 1234, "time": "2018-02-16T01:25:40.647000Z"
	    }

		gdax_producer.on_message(full_msg)
		msg = full_msg.copy()
		msg.pop('maker_order_id',None)
		msg.pop('taker_order_id', None)
		gdax_producer._kafka_producer.send.assert_called_once_with('gdax',value=msg,key='LTC-EUR')

		assert 'maker_order_id' not in msg
		assert 'taker_order_id' not in msg


	def test_kafka_send_error_stops_the_producer(self):
		from kafka.errors import ConnectionError
		gdax_producer = GdaxKafkaProducer("gdax", {'products': ['LTC-EUR']}, {})
		gdax_producer._kafka_producer = MagicMock()
		future_mock = gdax_producer._kafka_producer.send.return_value
		future_mock.get.side_effect = ConnectionError

		gdax_producer.stop = MagicMock()

		with pytest.raises(ConnectionError):
			gdax_producer.on_message({'type': 'match', 'product_id': 'LTC-EUR'})

		gdax_producer.stop.assert_called_once()


	def test__create_with_environment(self):
		os.environ['CRYPTO_GDAX_PRODUCTS'] = "LTC-EUR"
		os.environ['CRYPTO_GDAX_CHANNELS'] = "ticker"
		os.environ['CRYPTO_GDAX_TIMEOUT'] = "2"

		os.environ['CRYPTO_KAFKA_BOOTSTRAP_SERVERS'] = 'localhost:9092'
		os.environ['CRYPTO_KAFKA_TOPIC'] = 'gdax'
		os.environ['CRYPTO_KAFKA_MATCHES_ONLY'] = 'true'

		client = GdaxKafkaProducer.create_with_environment()

		assert client._products == ['LTC-EUR']
		assert client._channels == ['ticker']
		assert client._timeout == 2
		assert client._kafka_topic == 'gdax'
		assert client._kafka_kwargs['bootstrap_servers'] == ['localhost:9092']
		assert client._matches_only == True