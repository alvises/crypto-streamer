"""
    File name: test_gdax.py
    Author: Alvise Susmel <alvise@poeticoding.com>

    GdaxStreamer unit test
"""

import pytest
import json
from mock import MagicMock

from gdax.streamer import GdaxStreamer, NoChannelsError, NoProductsError
from websocket import WebSocketTimeoutException, \
    WebSocketConnectionClosedException, WebSocketAddressException

@pytest.fixture
def gdax_matches(): return GdaxStreamer(['LTC-EUR'])


@pytest.fixture
def last_match_msg():
    return {
        "type": "last_match", "trade_id": 2562730,
        "maker_order_id": "ce6504d8-44b5-44ab-8e2e-487a15931e59",
        "taker_order_id": "44547f40-6a6f-4b7d-a993-9b1747d75408",
        "side": "sell", "size": "3.53526947",
        "sequence": 463248628, "time": "2018-02-16T01:25:40.647000Z"
    }

# @pytest.fixture
# def match_msg():


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
        products = ['BTC-EUR','ETH-EUR']
        gdax = GdaxStreamer(products=products)

        message = gdax._subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscribe'
        assert m['product_ids'].sort() == products.sort()



    def test__subscribtion_message__valid_channel(self):
        gdax = GdaxStreamer(['BTC-EUR'],channels=['matches'] )

        message = gdax._subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscribe'
        assert 'matches' in m['channels']



    def test__subscription_message__adds_heartbeat_to_channel_list(self):
        gdax = GdaxStreamer(['BTC-EUR'], channels=['matches'])
        msg = json.loads(gdax._subscription_message())

        assert 'heartbeat' in msg['channels']



    def test__subscription_message__no_duplicate_channels(self):
        gdax = GdaxStreamer(['BTC-EUR'], channels=['matches','heartbeat','matches'])
        msg = json.loads(gdax._subscription_message())

        assert msg['channels'].sort() == ['matches','heartbeat'].sort()



    def test__subscription_message__no_duplicate_products(self):
        gdax = GdaxStreamer(['BTC-EUR','LTC-EUR','BTC-EUR'])
        msg = json.loads(gdax._subscription_message())

        assert msg['product_ids'].sort() == ['BTC-EUR','LTC-EUR'].sort()



    def test_init__raises_exception_with_no_products(self):
        with pytest.raises(NoProductsError):
            GdaxStreamer([])



    def test__init__raises_exception_with_no_channels(self):
        with pytest.raises(NoChannelsError):
            GdaxStreamer(["ETH-EUR"],[])



    def test__connect__connects_to_gdax(self):
        gdax = GdaxStreamer(['ETH-EUR'],['matches'],30)
        gdax._create_connection = MagicMock()
        gdax._connect()

        gdax._create_connection \
            .assert_called_once_with("wss://ws-feed.gdax.com",timeout=30)



    def test__subscribe__sends_subscription_message(self):
        gdax = GdaxStreamer(['ETH-EUR'],['matches'],30)
        gdax._ws = MagicMock()

        gdax._subscribe()

        gdax._ws.send.assert_called_with(json.dumps({
            'type': 'subscribe',
            'product_ids': ['ETH-EUR'],
            'channels': list(set(['matches','heartbeat']))
        }))



    def test__handle_message__proxy_each_message(self,gdax_matches):
        gdax_matches.on_message = MagicMock()

        gdax_matches._handle_message({'type': 'subscriptions'})

        gdax_matches.on_message.assert_called_once_with({'type': 'subscriptions'})



    def test__handle_message__handles_last_match(self,gdax_matches,last_match_msg):
        gdax_matches.on_last_match = MagicMock()
        gdax_matches._handle_message(last_match_msg)
        gdax_matches.on_last_match.assert_called_once_with(last_match_msg)



    def test__handle_message__handles_subscriptions_response(self,gdax_matches,subscription_response_msg):
        gdax_matches.on_subscriptions = MagicMock()

        gdax_matches._handle_message(subscription_response_msg)

        gdax_matches.on_subscriptions \
                    .assert_called_once_with(subscription_response_msg)



    def test__handle_message__handles_match(self,gdax_matches,match_msg):
        gdax_matches.on_match= MagicMock()
        gdax_matches._handle_message(match_msg)
        gdax_matches.on_match.assert_called_once_with(match_msg)


    def test__connect__connection_error_raises_the_error(self,gdax_matches):
        gdax_matches._create_connection = MagicMock(side_effect=WebSocketAddressException)
        gdax_matches.on_connection_error = MagicMock()
        with pytest.raises(WebSocketAddressException):
            gdax_matches._connect()



    def test__subscribe__connection_error_triggers_on_connection_error_is_called(self,gdax_matches):
        gdax_matches._ws = MagicMock()
        gdax_matches._ws.send.side_effect = WebSocketConnectionClosedException
        gdax_matches.on_connection_error = MagicMock()
        gdax_matches._subscribe()
        gdax_matches.on_connection_error.assert_called_once()


    def test__mainloop__timeout_exception_triggers_on_connection_error(self,gdax_matches):
        gdax_matches._ws = MagicMock()
        gdax_matches.on_connection_error = MagicMock()
        gdax_matches._ws.recv.side_effect = WebSocketTimeoutException
        gdax_matches._mainloop()
        gdax_matches.on_connection_error.assert_called_once()



    def test__mainloop__connection_exception_triggers_on_connection_error(self, gdax_matches):
        gdax_matches._ws = MagicMock()
        gdax_matches.on_connection_error = MagicMock()
        gdax_matches._ws.recv.side_effect = WebSocketConnectionClosedException
        gdax_matches._mainloop()
        gdax_matches.on_connection_error.assert_called_once()




            # test connection to kafka
    # every message received is sent to kafka GDAX-TOPIC

    # the kafka key should be the product

    # does the subscription should be sent


