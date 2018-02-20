"""
	File name: provider.py
	Author: Alvise Susmel <alvise@poeticoding.com>

	ProviderClient is an interface to implement a client to get realtime data.
"""
from os import getenv, environ

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

	@classmethod
	def get_str_from_env(cls,key):
		return getenv(key,None)

	@classmethod
	def get_int_from_env(cls,key):
		if environ.get(key) is None: return
		s = cls.get_str_from_env(key)
		return int(s)


	@classmethod
	def get_list_from_env(cls,key):
		if environ.get(key) is None: return
		return list(filter(
				lambda s: s.strip(),
				map(
					lambda p: p.strip(),
					getenv(key).split(',')
				)
		))


	@classmethod
	def get_boolean_from_env(cls,key):
		v = environ.get(key)
		if v is None: return
		return v.strip().lower() == 'true'

