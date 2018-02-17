from cryptostreamer.gdax import GDAXKafkaProducer

from mock import MagicMock


def test__start__it_tries_to_connect_to_kafka_before_connecting_to_gdax():
	gdax_producer = GDAXKafkaProducer()
	gdax_producer._connect = MagicMock()


# stop disconnects from gdax and kafka
# every message is published to kafka
# reconnects automatically after connection error in handle mainloop

# env variables for kafka
# env variables for gdax products, channels and timeout
