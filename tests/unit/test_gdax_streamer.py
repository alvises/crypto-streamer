"""
    File name: test_gdax.py
    Author: Alvise Susmel <alvise@poeticoding.com>

    GdaxStreamer unit test
"""

import pytest
import json
from mock import MagicMock


@pytest.fixture
def last_match_msg():
    return {
        "type": "last_match", "trade_id": 2562730,
        "maker_order_id": "ce6504d8-44b5-44ab-8e2e-487a15931e59",
        "taker_order_id": "44547f40-6a6f-4b7d-a993-9b1747d75408",
        "side": "sell", "size": "3.53526947",
        "sequence": 463248628, "time": "2018-02-16T01:25:40.647000Z"
    }

@pytest.fixture
def subscription_response_msg():
    return {
        "type": "subscriptions",
        "channels": [
            {
                "name": "matches",
                "product_ids": ["LTC-EUR"]
            },
            {
                "name": "heartbeat",
                "product_ids": ["LTC-EUR"]
            }
        ]
    }



class TestGdaxStreamer:

    def test__subscribtion_message__multiple_valid_products(self):
        from gdax.streamer import GdaxStreamer
        products = ['BTC-EUR','ETH-EUR']
        gdax = GdaxStreamer(products=products)

        message = gdax._subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscribe'
        assert m['product_ids'].sort() == products.sort()


    def test__subscribtion_message__valid_channel(self):
        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['BTC-EUR'],channels=['matches'] )

        message = gdax._subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscribe'
        assert 'matches' in m['channels']


    def test__subscription_message__adds_heartbeat_to_channel_list(self):
        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['BTC-EUR'], channels=['matches'])
        msg = json.loads(gdax._subscription_message())

        assert 'heartbeat' in msg['channels']

    def test__subscription_message__no_duplicate_channels(self):
        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['BTC-EUR'], channels=['matches','heartbeat','matches'])
        msg = json.loads(gdax._subscription_message())

        assert msg['channels'].sort() == ['matches','heartbeat'].sort()

    def test__subscription_message__no_duplicate_products(self):
        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['BTC-EUR','LTC-EUR','BTC-EUR'])
        msg = json.loads(gdax._subscription_message())

        assert msg['product_ids'].sort() == ['BTC-EUR','LTC-EUR'].sort()

    def test_init__raises_exception_with_no_products(self):
        from gdax.streamer import GdaxStreamer, NoProductsError
        with pytest.raises(NoProductsError):
            GdaxStreamer([])


    def test__init__raises_exception_with_no_channels(self):
        from gdax.streamer import GdaxStreamer, NoChannelsError
        with pytest.raises(NoChannelsError):
            GdaxStreamer(["ETH-EUR"],[])


    def test__connect__connects_to_gdax(self):
        import gdax.streamer

        create_connection_mock = MagicMock()
        gdax.streamer.create_connection = create_connection_mock


        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['ETH-EUR'],['matches'],30)
        gdax._connect()

        create_connection_mock.assert_called_once_with("wss://ws-feed.gdax.com",timeout=30)


    def test__subscribe__sends_subscription_message(self):
        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['ETH-EUR'],['matches'],30)

        ws_mock = MagicMock()
        gdax._ws = ws_mock

        gdax._subscribe()

        subscription_msg = json.dumps({
            'type': 'subscribe',
            'product_ids': ['ETH-EUR'],
            'channels': list(set(['matches','heartbeat']))
        })

        ws_mock.send.assert_called_with(subscription_msg)


    def test__handle_message__proxy_each_message(self):
        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['ETH-EUR'])

        on_message_mock = MagicMock()
        gdax.on_message = on_message_mock

        last_message = {'type': 'subscriptions'}
        gdax._handle_message(last_message)

        gdax.on_message.assert_called_once_with(last_message)


    def test__handle_message__handles_last_match(self,last_match_msg):
        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['LTC-EUR'])

        on_last_match_mock = MagicMock()
        gdax.on_last_match = on_last_match_mock

        gdax._handle_message(last_match_msg)

        on_last_match_mock.assert_called_once_with(last_match_msg)


    def test__handle_message__handles_subscriptions_response(self,subscription_response_msg):
        from gdax.streamer import GdaxStreamer
        gdax = GdaxStreamer(['LTC-EUR'])

        on_subscriptions_mock = MagicMock()
        gdax.on_subscriptions = on_subscriptions_mock

        gdax._handle_message(subscription_response_msg)

        on_subscriptions_mock.assert_called_once_with(subscription_response_msg)




        # test real connection failure and mock it, during connection
    # test real connection timeout/failure when receiving data

    # heartbeat ?

    # test connection to kafka
    # every message received is sent to kafka GDAX-TOPIC

    # the kafka key should be the product

    # does the subscription should be sent


