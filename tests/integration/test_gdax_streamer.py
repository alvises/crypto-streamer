import pytest, json
from gdax.streamer import GdaxStreamer

def test__connect_and_get_subscription_confirmation():
	gdax = GdaxStreamer(['LTC-EUR'],['matches'])
	gdax._connect()
	gdax._subscribe()

	last_trade_res = gdax._ws.recv()
	subscriptions_res = gdax._ws.recv()
	print(last_trade_res)
	r = json.loads(subscriptions_res)
	assert r['type'] == 'subscriptions'


def test__connect_and_get_last_match():
	gdax = GdaxStreamer(['LTC-EUR'],['matches'])
	gdax._connect()
	gdax._subscribe()

	_ = gdax._ws.recv()
	subscriptions_res = gdax._ws.recv()
	print(subscriptions_res)


