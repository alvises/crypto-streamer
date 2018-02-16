"""
	File name: gdax.py
	Author: Alvise Susmel <alvise@poeticoding.com>
"""

from websocket import create_connection, WebSocketTimeoutException
import json
import logging
import time


class NoProductsError(Exception): pass
class NoChannelsError(Exception): pass


LOGGER = logging.getLogger('GdaxStreamer')
LOGGER.setLevel(logging.INFO)

GDAX_WSS_URL = 'wss://ws-feed.gdax.com'
DEFAULT_WS_TIMEOUT = 30


class GdaxStreamer():

	def __init__(self,products,channels=['matches'],timeout=30):
		self._products = products
		self._channels = channels
		if len(self._products) == 0: raise NoProductsError()
		if len(self._channels) == 0: raise NoChannelsError()
		self._timeout = timeout or DEFAULT_WS_TIMEOUT


	def start(self):
		"""
		Websocket client connects to GDAX server to the realtime tick data.
		Tick data is then streamed into Kafka GDAX topic.
		"""
		self._stop = False


	def _connect(self):
		self._ws = create_connection(GDAX_WSS_URL, timeout=self._timeout)
		self._ws.send(self._subscription_message())

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

