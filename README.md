# dhh

Hola! This readme file will guide you how to setup my test answer in your local environment.

Step 1:
mkdir dhh && cd ff
Clone this repository

Step 2:
https://cloud.google.com/dataproc/docs/quickstarts/quickstart-console
Create a dataproc cluster with cluster name `dhh-cluster` and region `us-central1`

Step 3: Setting up docker
Build docker file:
`docker build --tag dhh-app .`

Run container with bash and exposing ports 5000:5000

`docker run -ti --name dhh -p 5000:5000 dhh-app:latest bash`

Authenticate using `gcloud init`

Run flask app with command:
`flask run --host 0.0.0.0`

Step 3:

Open another terminal or postman desktop client to make api call
curl http://127.0.0.1:5000/user?fullvisitorid={}







docker build --tag dhh-app .
docker run -ti --name raghav_dhh dhh-app:latest bash

gcloud init

flask run --host 0.0.0.0
