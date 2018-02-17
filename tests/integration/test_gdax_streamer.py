import pytest, json
from gdax.streamer import GdaxStreamer

def test__connect_and_get_subscription_confirmation():
	gdax = GdaxStreamer(['LTC-EUR'],['matches'])
	gdax._connect()
	gdax._subscribe()

	_ = gdax._ws.recv()
	subscriptions_res = gdax._ws.recv()
	r = json.loads(subscriptions_res)

	assert r['type'] == 'subscriptions'



def test__connect_and_get_last_match():
	gdax = GdaxStreamer(['LTC-EUR'],['matches'])
	gdax._connect()
	gdax._subscribe()
	msg = gdax._ws.recv()

	assert 'last_match' in json.loads(msg)['type']


def test__connects__get_last_match__disconnect():
	gdax = GdaxStreamer(['LTC-EUR'])
	def get_last_match_and_disconnect(last_match):
		print("LAST MATCH",last_match)
		gdax.disconnect()
	gdax.on_last_match = get_last_match_and_disconnect
	gdax.start()

	assert not gdax._mainloop_running
