"""
    File name: test_gdax.py
    Author: Alvise Susmel <alvise@poeticoding.com>

    GdaxStreamer unit test
"""

import binascii
import json
import os
import random
import datetime

import pytest
from mock import MagicMock
from freezegun import freeze_time

from websocket import WebSocketTimeoutException, \
    WebSocketConnectionClosedException, WebSocketAddressException

from cryptostreamer.gdax.client import GdaxClient, NoChannelsError, NoProductsError


def random_order_id():
    return "%s-%s-%s-%s-%s" % (
        binascii.b2a_hex(os.urandom(8)), binascii.b2a_hex(os.urandom(4)),
        binascii.b2a_hex(os.urandom(4)),binascii.b2a_hex(os.urandom(4)),
        binascii.b2a_hex(os.urandom(8))
    )

def random_order_id():
    return int(random.random()*1000000)

def random_sequence():
    return int(random.random() * 1000000000)

@pytest.fixture
def gdax_matches(): return GdaxClient(['LTC-EUR'])


@pytest.fixture
def match_msg():
    return {
        "type": "match", "trade_id": random_order_id(),
        "maker_order_id": random_order_id(),
        "taker_order_id": random_order_id(),
        "side": "sell", "size": "3.53526947",
        "price": "183.80000000", "product_id": "LTC-EUR",
        "sequence": random_sequence(), "time": "2018-02-16T01:25:40.647000Z"
    }

@pytest.fixture
def last_match_msg(match_msg):
    match_msg['type'] = "last_match"
    return match_msg


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


@pytest.fixture
def heartbeat_msg():
    return {
        'type': 'heartbeat', 'last_trade_id': 12174997,
        'product_id': 'BTC-EUR', 'sequence': 3376717970,
        'time': '2018-02-17T21:09:45.371000Z'
    }


class TestGdaxStreamer:

    def test__subscribtion_message__multiple_valid_products(self):
        products = ['BTC-EUR','ETH-EUR']
        gdax = GdaxClient(products=products)

        message = gdax._subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscribe'
        assert m['product_ids'].sort() == products.sort()



    def test__subscribtion_message__valid_channel(self):
        gdax = GdaxClient(['BTC-EUR'],channels=['matches'] )

        message = gdax._subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscribe'
        assert 'matches' in m['channels']



    def test__subscription_message__adds_heartbeat_to_channel_list(self):
        gdax = GdaxClient(['BTC-EUR'], channels=['matches'])
        msg = json.loads(gdax._subscription_message())

        assert 'heartbeat' in msg['channels']



    def test__subscription_message__no_duplicate_channels(self):
        gdax = GdaxClient(['BTC-EUR'], channels=['matches','heartbeat','matches'])
        msg = json.loads(gdax._subscription_message())

        assert msg['channels'].sort() == ['matches','heartbeat'].sort()


    def test__subscription_message__no_duplicate_products(self):
        gdax = GdaxClient(['BTC-EUR','LTC-EUR','BTC-EUR'])
        msg = json.loads(gdax._subscription_message())

        assert msg['product_ids'].sort() == ['BTC-EUR','LTC-EUR'].sort()



    def test_init__raises_exception_with_no_products(self):
        with pytest.raises(NoProductsError):
            GdaxClient([])



    def test__init__raises_exception_with_no_channels(self):
        with pytest.raises(NoChannelsError):
            GdaxClient(["ETH-EUR"],[])



    def test__connect__connects_to_gdax(self):
        gdax = GdaxClient(['ETH-EUR'],['matches'],30)
        gdax._create_connection = MagicMock()
        gdax._connect()

        gdax._create_connection \
            .assert_called_once_with("wss://ws-feed.gdax.com",timeout=30)


    def test__on_connected__called_when_ws_connected(self,gdax_matches):
        gdax_matches._create_connection = MagicMock()
        gdax_matches.on_connected = MagicMock()
        gdax_matches._connect()
        gdax_matches.on_connected.assert_called_once()


    def test__subscribe__sends_subscription_message(self):
        gdax = GdaxClient(['ETH-EUR'],['matches'],30)
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


    def test__heartbeat__sent_to_on_message(self,gdax_matches,heartbeat_msg):
        gdax_matches.on_message = MagicMock()
        gdax_matches._handle_message(heartbeat_msg)
        gdax_matches.on_message.assert_called_once_with(heartbeat_msg)


    def test__handle_message__handles_heartbeat(self,gdax_matches,heartbeat_msg):
        gdax_matches.on_heartbeat = MagicMock()
        gdax_matches._handle_message(heartbeat_msg)
        gdax_matches.on_heartbeat.assert_called_once_with(heartbeat_msg)


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
        gdax_matches._mainloop_recv_msg()
        gdax_matches.on_connection_error.assert_called_once()



    def test__mainloop__connection_exception_triggers_on_connection_error(self, gdax_matches):
        gdax_matches._ws = MagicMock()
        gdax_matches.on_connection_error = MagicMock()
        gdax_matches._ws.recv.side_effect = WebSocketConnectionClosedException
        gdax_matches._mainloop_recv_msg()
        gdax_matches.on_connection_error.assert_called_once()


    def test__mainloop__sends_keepalive_ping_at_least_10s_between_the_pings(self,gdax_matches):
        gdax_matches._ws = MagicMock()
        gdax_matches._ws.recv.return_value = "{}"
        ping_mock = gdax_matches._ws.ping
        with freeze_time("2018-02-18 12:00:00"):
            gdax_matches._pinged_at = datetime.datetime.now()
            gdax_matches._mainloop_recv_msg()
        ping_mock.assert_not_called()

        with freeze_time("2018-02-18 12:00:11"):
            gdax_matches._mainloop_recv_msg()
        ping_mock.assert_called_once_with('keepalive')


