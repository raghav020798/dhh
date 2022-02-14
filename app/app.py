import logging
import json
import re
from flask import Flask
from flask_restful import Resource, Api, reqparse
import subprocess

__log_format__ = ("[%(asctime)s] [%(levelname)s] "
                  "[dhh: %(name)s.%(funcName)s:%(lineno)d]: %(message)s")

logging.basicConfig(format=__log_format__, level=logging.INFO, datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

pat = r'.*?\##(.*)##.*'

app = Flask(__name__)
api = Api(app)

class User(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('fullvisitorid', required=True)
        args = parser.parse_args()
        
        logger.info('Sending dataproc job')

        process = subprocess.Popen(['sh','spark_submit.sh',args.fullvisitorid],
                                         stdout = subprocess.PIPE,
                                         stderr = subprocess.PIPE)
        log_ouput=''
        for line in process.stderr: 
            print(line.decode("utf-8"), end='')
            log_ouput+=line.decode("utf-8")
        process.wait()

        stdout, stderr = process.communicate() 
        match = re.search(pat, log_ouput)

        if match:

            return match.group(1)
        
        return []

api.add_resource(User, '/user')

if __name__ == '__main__':
    app.run(debug=True)