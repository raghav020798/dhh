import argparse
import logging
from etl import process

__log_format__ = ("[%(asctime)s] [%(levelname)s] "
                  "[dhh: %(name)s.%(funcName)s:%(lineno)d]: %(message)s")

logging.basicConfig(format=__log_format__, 
	level=logging.INFO, datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

response_template = """
+======================================+
| Full visitor id : {visitor_id}
+--------------------------------------+
| order_placed    : {order_placed}
| 
| address_changed : {address_changed}
| 
| order_delivered : {order_delivered}
| 
| application_type: {application_type}
+======================================+
"""

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-v", "--visitor_id", help='Full visitor id', required=True)
	args = parser.parse_args()
	response = process(visitor_id = args.visitor_id)

	logger.info('+--------------------------------------------------+')
	logger.info(' Response received for visitor: {}'.format(args.visitor_id))
	logger.info('+--------------------------------------------------+')
	for res in response:
		logger.info(response_template.format(visitor_id=args.visitor_id,
											order_placed=res['order_placed'],
											address_changed=res['address_changed'],
											order_delivered=res['order_delivered'],
											application_type=res['application_type'],
											))


if __name__=='__main__':
	main()