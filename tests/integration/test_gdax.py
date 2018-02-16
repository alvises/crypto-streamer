import pytest
from streamer.gdax import GdaxStreamer

def test__connect_and_get_subscription_confirmation():
	gdax = GdaxStreamer([''])