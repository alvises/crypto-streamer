"""
	File name: gdax.py
	Author: Alvise Susmel <alvise@poeticoding.com>
"""

import json
import logging
import time
from websocket import create_connection, WebSocketTimeoutException, WebSocketConnectionClosedException

class NoProductsError(Exception): pass
class NoChannelsError(Exception): pass


LOGGER = logging.getLogger('GdaxStreamer')
LOGGER.setLevel(logging.INFO)

GDAX_WSS_URL = 'wss://ws-feed.gdax.com'
DEFAULT_WS_TIMEOUT = 30


class GdaxStreamer():

	def __init__(self,products,channels=['matches'],timeout=30):
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
		self._connect()
		self._subscribe()
		return self._mainloop()



	def disconnect(self):
		self._mainloop_running = False
		self._ws.close()
		self._ws = None


	def on_message(self, msg):
		"""
		Callback for all the messages.
		"""
		return

	def on_last_match(self,last_match):
		"""
		Callback for last_match msg.
		Once connected and subscribed, a last match message is sent.

		:param last_match: dict
		"""
		return

	def on_subscriptions(self,subscriptions_msg):
		"""
		Once the subscription message is sent, an subscriptions_msg answer is sent.
		:param subscriptions_msg: dict
		"""
		return

	def on_match(self,match_msg):
		"""
		Implement this callback to get each trade.
		:param match_msg:
		"""
		return


	def on_connection_error(self,e):
		"""
		Called when a connection error during subscription or mainloop is caught.
		If not implemented, it raises the exception

		:param e: exception
		:return: None, if True it reconnects automatically
		"""
		raise e




	def _connect(self):
		self._ws = self._create_connection(GDAX_WSS_URL, timeout=self._timeout)


	def _subscribe(self):
		try:
			self._ws.send(self._subscription_message())
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


	def _handle_message(self,msg):
		"""
		Handles all the message and proxy them to callbacks.
		:param msg: dict
		"""
		self.on_message(msg)
		msg_type = msg.get('type')
		if msg_type == 'last_match':
			self.on_last_match(msg)
		elif msg_type == 'subscriptions':
			self.on_subscriptions(msg)
		elif msg_type == 'match':
			self.on_match(msg)


	def _mainloop(self):
		"""
		The mainloop receives loops and gets and handles
		the messages from GDAX.
		It sends a ping every 30 seconds.
		"""
		self._mainloop_running = True

		while self._mainloop_running:
			try:
				data = self._ws.recv()
			except Exception as e:
				# if on_error returns True
				# it reconnects automatically
				return self.on_connection_error(e)

			msg = json.loads(data)
			self._handle_message(msg)