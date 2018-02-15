"""
    File name: test_gdax.py
    Author: Alvise Susmel <alvise@poeticoding.com>

    GdaxStreamer unit test
"""

import pytest
import json
from mock import MagicMock
from streamer.gdax import GdaxStreamer


class TestGdaxStreamer:

    def test_subscribtion_message__multiple_valid_products(self):

        products = ['BTC-EUR','ETH-EUR']
        gdax = GdaxStreamer(products=products)

        message = gdax.subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscription'
        assert m['product_ids'] == products


    def test_subscribtion_message__valid_channel(self):
        channels = ['matches']
        gdax = GdaxStreamer(['BTC-EUR'],channels=channels )

        message = gdax.subscription_message()
        m = json.loads(message)

        assert m['type'] == 'subscription'
        assert m['channels'] == channels


    def test_init__raises_exception_with_no_products(self):
        from streamer.gdax import NoProductsError
        with pytest.raises(NoProductsError):
            GdaxStreamer([])


    def test_init__raises_exception_with_no_channels(self):
        from streamer.gdax import NoChannelsError
        with pytest.raises(NoChannelsError):
            GdaxStreamer(["ETH-EUR"],[])



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


