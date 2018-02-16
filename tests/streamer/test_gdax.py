"""
    File name: test_gdax.py
    Author: Alvise Susmel <alvise@poeticoding.com>

    GdaxStreamer unit test
"""

import pytest
import json
from mock import MagicMock


class TestGdaxStreamer:

    def test__subscribtion_message__multiple_valid_products(self):
        from streamer.gdax import GdaxStreamer
        products = ['BTC-EUR','ETH-EUR']
        gdax = GdaxStreamer(products=products)

        message = gdax._subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscribe'
        assert m['product_ids'].sort() == products.sort()


    def test__subscribtion_message__valid_channel(self):
        from streamer.gdax import GdaxStreamer
        gdax = GdaxStreamer(['BTC-EUR'],channels=['matches'] )

        message = gdax._subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscribe'
        assert 'matches' in m['channels']


    def test__subscription_message__adds_heartbeat_to_channel_list(self):
        from streamer.gdax import GdaxStreamer
        gdax = GdaxStreamer(['BTC-EUR'], channels=['matches'])
        msg = json.loads(gdax._subscription_message())

        assert 'heartbeat' in msg['channels']

    def test__subscription_message__no_duplicate_channels(self):
        from streamer.gdax import GdaxStreamer
        gdax = GdaxStreamer(['BTC-EUR'], channels=['matches','heartbeat','matches'])
        msg = json.loads(gdax._subscription_message())

        assert msg['channels'].sort() == ['matches','heartbeat'].sort()

    def test__subscription_message__no_duplicate_products(self):
        from streamer.gdax import GdaxStreamer
        gdax = GdaxStreamer(['BTC-EUR','LTC-EUR','BTC-EUR'])
        msg = json.loads(gdax._subscription_message())

        assert msg['product_ids'].sort() == ['BTC-EUR','LTC-EUR'].sort()

    def test_init__raises_exception_with_no_products(self):
        from streamer.gdax import GdaxStreamer, NoProductsError
        with pytest.raises(NoProductsError):
            GdaxStreamer([])


    def test__init__raises_exception_with_no_channels(self):
        from streamer.gdax import GdaxStreamer, NoChannelsError
        with pytest.raises(NoChannelsError):
            GdaxStreamer(["ETH-EUR"],[])


    def test__connect__connects_to_gdax_sending_subscription_message(self):
        import websocket
        create_connection_mock = MagicMock()
        websocket.create_connection = create_connection_mock
        from streamer.gdax import GdaxStreamer

        gdax = GdaxStreamer(['ETH-EUR'],['matches'],30)
        gdax._connect()

        create_connection_mock.assert_called_once_with("wss://ws-feed.gdax.com",timeout=30)
        ws_mock = create_connection_mock.return_value

        subscription_msg = json.dumps({
            'type': 'subscribe',
            'product_ids': ['ETH-EUR'],
            'channels': ['matches','heartbeat']
        })

        ws_mock.send.assert_called_with(subscription_msg)





    # test real connection failure and mock it, during connection
    # test once opened send subscription message

    # once sent subscription message -> subscription confirmation

    # handle first message


    # test real connection failure and mock it, during connection
    # test real connection timeout/failure when receiving data

    # heartbeat ?

    # test connection to kafka
    # every message received is sent to kafka GDAX-TOPIC

    # the kafka key should be the product

    # does the subscription should be sent


