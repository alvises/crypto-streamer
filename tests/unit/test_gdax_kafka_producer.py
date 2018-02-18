import pytest
from cryptostreamer.gdax import GdaxKafkaProducer
from mock import MagicMock


def test__start__it_tries_to_connect_to_kafka_before_connecting_to_gdax():
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


def test__disconnect__disconnects_from_gdax_and_kafka():
	gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']})
	ws_mock = MagicMock()
	kp_mock = MagicMock()
	gdax_producer._ws = ws_mock
	gdax_producer._kafka_producer = kp_mock

	gdax_producer._disconnect()

	ws_mock.close.assert_called_once()
	kp_mock.close.assert_called_once()


def test_if__matches_only__option_is_True_then_only_match_msg_are_published_to_kafka():
	gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{},matches_only=True)
	gdax_producer._kafka_producer = MagicMock()

	gdax_producer.on_message({'type': 'last_match', 'product_id': 'BTC-USD'})
	gdax_producer._kafka_producer.send.assert_not_called()

	gdax_producer.on_message({'type': 'match', 'product_id': 'BTC-USD'})
	gdax_producer._kafka_producer.send.assert_called_once()


def test_hearbeat_messages_are_never_published_to_kafka():
	gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{})
	gdax_producer._kafka_producer = MagicMock()
	gdax_producer.on_message({'type': 'heartbeat'})
	gdax_producer._kafka_producer.send.assert_not_called()

	gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{},matches_only=True)
	gdax_producer._kafka_producer = MagicMock()
	gdax_producer.on_message({'type': 'heartbeat'})
	gdax_producer._kafka_producer.send.assert_not_called()

def test_subscriptions_messages_are_never_published_to_kafka():
	gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{})
	gdax_producer._kafka_producer = MagicMock()
	gdax_producer.on_message({'type': 'subscriptions'})
	gdax_producer._kafka_producer.send.assert_not_called()



def test_when_sending_to_kafka_the_key_is_always_the_product_id():
	# the reason is because having product id as key enforce the order of the received trades
	# all the messages of one product will go in just one partition

	gdax_producer = GdaxKafkaProducer("gdax",{'products': ['BTC-USD']},{})
	gdax_producer._kafka_producer = MagicMock()

	msg = {'type': 'match', 'product_id': 'BTC-USD'}
	gdax_producer.on_message(msg)
	gdax_producer._kafka_producer.send.assert_called_once_with('gdax',value=msg,key='BTC-USD')


def test_order_id_are_filtered():
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