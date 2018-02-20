import sys
from optparse import OptionParser
parser = OptionParser()

parser.add_option("--gdax", dest="gdax", action="store_true",
                  default=False, help="Streams GDAX data into Kafka (set ENV)")


(options, args) = parser.parse_args()

if options.gdax:
	from cryptostreamer.gdax import GdaxKafkaProducer
	gdax = GdaxKafkaProducer.create_with_environment()
	gdax.start()