

def run_gdax():
	from cryptostreamer.gdax import GdaxKafkaProducer
	gdax = GdaxKafkaProducer.create_with_environment()
	gdax.start()


from os import getenv

provider = getenv('CRYPTO_STREAMER_PROVIDER',None)
if provider == 'gdax': run_gdax()