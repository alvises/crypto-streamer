"""
	File name: provider.py
	Author: Alvise Susmel <alvise@poeticoding.com>

	ProviderClient is an interface to implement a client to get realtime data.
"""


class ProviderClient(object):

	def start(self):
		"""
		Client connects to the provider and starts to get the data from.

		:return: None or on_error callback return
		"""
		pass


	def stop(self):
		"""
		Closes the connection and ends the mainloop, if any.

		"""
		pass


	def on_message(self, msg):
		"""
		Callback for all the messages.
		"""
		pass


	def on_connection_error(self,e):
		"""
		Called when a connection error.
		If not implemented, it raises the exception

		:param e: exception
		"""
		raise e
