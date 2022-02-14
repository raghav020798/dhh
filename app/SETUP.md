**Step 1: Clone this repository**
```
git clone https://github.com/raghav020798/dhh.git
cd dhh/app
```

**Step 2: Create a dataproc cluster with cluster name `dhh-cluster` and region `us-central1`**
-Refer this https://cloud.google.com/dataproc/docs/quickstarts/quickstart-console


**Step 3: Setting up docker**

- Build docker file:
`docker build --tag dhh-app .`

- Run container with bash and exposing ports 5000:5000
`docker run -ti --name dhh -p 5000:5000 dhh-app:latest bash`

- Authenticate using `gcloud init` and set project_id where dataproc cluster was created.

- Run flask app with command:
`flask run --host 0.0.0.0`

**Step 4: Open another terminal or postman desktop client to make api call**

`curl http://127.0.0.1:5000/user?fullvisitorid={}`



