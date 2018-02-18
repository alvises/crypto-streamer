"""
	File name: gdax/client.py
	Author: Alvise Susmel <alvise@poeticoding.com>

	Implementation of GDAX Client to get realtime data.
"""

import json
import logging, sys
from datetime import datetime, timedelta

from websocket import create_connection, WebSocketTimeoutException, WebSocketConnectionClosedException
from cryptostreamer.provider import ProviderClient


class NoProductsError(Exception): pass
class NoChannelsError(Exception): pass


logging.basicConfig(stream=sys.stdout)
LOGGER = logging.getLogger('GdaxClient')
LOGGER.setLevel(logging.INFO)


GDAX_WSS_URL = 'wss://ws-feed.gdax.com'
DEFAULT_WS_TIMEOUT = 30


class GdaxClient(ProviderClient):

	def __init__(self,products=[],channels=['matches'],timeout=30):
		self._create_connection = create_connection
		self._products = products
		self._channels = channels
		if len(self._products) == 0: raise NoProductsError()
		if len(self._channels) == 0: raise NoChannelsError()
		self._timeout = timeout or DEFAULT_WS_TIMEOUT


	def start(self):
		"""
		Websocket client connects to GDAX server to the realtime tick data.
		Tick data is then streamed into Kafka GDAX topic.

		:return: None or on_error callback return
		"""
		self.on_setup()
		self._connect()
		self._subscribe()
		return self._mainloop()



	def stop(self):
		try:
			self._mainloop_running = False
			self._disconnect()
		except: pass



	def on_setup(self):
		"""
		Called before connecting to the provider
		"""
		pass




	def on_message(self, msg):
		"""
		Callback for all the messages.
		"""
		LOGGER.debug("recv: %s" %msg)
		pass


	def on_heartbeat(self,heartbeat_msg):
		"""
		Callback to get heartbeat every second

		"""
		pass

	def on_last_match(self,last_match):
		"""
		Callback for last_match msg.
		Once connected and subscribed, a last match message is sent.

		:param last_match: dict
		"""
		pass

	def on_subscriptions(self,subscriptions_msg):
		"""
		Once the subscription message is sent, an subscriptions_msg answer is sent.
		:param subscriptions_msg: dict
		"""
		pass

	def on_match(self,match_msg):
		"""
		Implement this callback to get each trade.
		:param match_msg:
		"""
		pass

	def on_connected(self):
		"""
		Called when connected to websocket.
		"""
		pass

	def on_disconnected(self):
		"""
		Called when disconnected from gdax.
		"""
		pass


	def on_connection_error(self,e):
		"""
		Called when a connection error during subscription or mainloop is caught.
		If not implemented, it raises the exception.

		:param e: exception
		:return: None, if True it reconnects automatically
		"""
		LOGGER.error(e)
		raise e




	def _connect(self):
		self._ws = self._create_connection(GDAX_WSS_URL, timeout=self._timeout)
		self.on_connected()


	def _disconnect(self):
		self._ws.close()
		self._ws = None
		self.on_disconnected()


	def _subscribe(self):
		try:
			subscription_msg = self._subscription_message()
			heartbeat_msg = self._heartbeat_message()

			LOGGER.info("send: %s" % subscription_msg)
			self._ws.send(subscription_msg)

			# LOGGER.info("send: %s" % heartbeat_msg)
			# self._ws.send(heartbeat_msg)

		except Exception as e:
			return self.on_connection_error(e)



	def _subscription_message(self):
		"""
		Subscription message based on products and channels.
		Heartbeat channel is added to have a validation of the sequence.

		:return: string
		"""
		return json.dumps({
			'type': 'subscribe',
			'product_ids': list(set(self._products)),
			'channels': list(set(self._channels + ['heartbeat']))
		})

	def _heartbeat_message(self):
		return json.dumps({"type": "heartbeat", "on": True})



	def _needs_ping(self):
		return self._pinged_at + timedelta(seconds=10) < datetime.now()


	def _ping(self):
		self._ws.ping('keepalive')
		self._pinged_at = datetime.now()




	def _handle_message(self,msg):
		"""
		Handles all the message and proxy them to callbacks.
		:param msg: dict
		"""
		self.on_message(msg)
		msg_type = msg.get('type')

		if msg_type == 'heartbeat':         self.on_heartbeat(msg)
		elif msg_type == 'last_match':      self.on_last_match(msg)
		elif msg_type == 'subscriptions':   self.on_subscriptions(msg)
		elif msg_type == 'match':           self.on_match(msg)


	def _mainloop(self):
		"""
		The mainloop receives loops and gets and handles
		the messages from GDAX.
		It sends a ping every 30 seconds.
		"""
		self._mainloop_running = True
		self._pinged_at = datetime.now()

		while self._mainloop_running:
			self._mainloop_recv_msg()


	def _mainloop_recv_msg(self):
		try:
			if self._needs_ping(): self._ping()
			data = self._ws.recv()
		except Exception as e:
			return self.on_connection_error(e)

		msg = json.loads(data)
		self._handle_message(msg)


if __name__ == "__main__":
	from os import environ

	def get_list_from_env(key):
		if key not in environ: return None
		return list(map(lambda v: v.strip(), environ[key].strip().split(',')))

	# ex BTC-EUR,LTC-EUR
	products = get_list_from_env('GDAX_CLIENT_PRODUCT_IDS')
	channels = get_list_from_env('GDAX_CLIENT_CHANNELS') or ['matches']
	timeout = get_list_from_env('GDAX_CLIENT_TIMEOUT') or DEFAULT_WS_TIMEOUT
	gdax = GdaxClient(products,channels,timeout)
	gdax.start()