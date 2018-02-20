from .provider import ProviderClient

import logging, sys
from os import getenv

def get_logger(name):
	logging.basicConfig(stream=sys.stdout)
	level = getenv("CRYPTO_STREAMER_LOG_LEVEL","INFO")

	l = logging.getLogger(name)
	if level == 'DEBUG':
		l.setLevel(logging.DEBUG)
	else:
		l.setLevel(logging.INFO)
	return l

