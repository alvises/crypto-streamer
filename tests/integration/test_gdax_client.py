import json
import pytest

from cryptostreamer.gdax.client import GdaxClient
from mock import MagicMock

def test__connect_and_get_subscription_confirmation():
	gdax = GdaxClient(['LTC-EUR'],['matches'])
	gdax._connect()
	gdax._subscribe()

	_ = gdax._ws.recv()
	subscriptions_res = gdax._ws.recv()
	r = json.loads(subscriptions_res)

	assert r['type'] == 'subscriptions'



def test__connect_and_get_last_match():
	gdax = GdaxClient(['LTC-EUR'],['matches'])
	gdax._connect()
	gdax._subscribe()
	msg = gdax._ws.recv()

	assert 'last_match' in json.loads(msg)['type']


def test__start__get_last_match__disconnect():
	gdax = GdaxClient(['LTC-EUR'])
	def get_last_match_and_disconnect(last_match):
		print("LAST MATCH",last_match)
		gdax.stop()
	gdax.on_last_match = get_last_match_and_disconnect
	gdax.start()

	assert not gdax._mainloop_running

def test_receives_heartbeats():
	from threading import Thread
	from time import sleep
	gdax = GdaxClient(['LTC-EUR'])
	gdax.on_heartbeat = MagicMock()
	try:
		thread = Thread(target=gdax.start)
		thread.start()
		sleep(5)
		gdax.stop()
		thread.join()
	except: pass

	gdax.on_heartbeat.assert_called()



@pytest.mark.skip('this can keep  minutes if no trades are made')
def test__start__get_match_and_disconnect():
	gdax = GdaxClient(['LTC-EUR'])
	def get_match_and_disconnect(msg):
		if msg['type'] == 'match':
			print("MATCH TRADE",msg)
			gdax.disconnect()

	gdax.on_message = get_match_and_disconnect
	gdax.start()

	assert not gdax._mainloop_running



