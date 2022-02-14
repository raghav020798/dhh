# dhh

Hola! This file will guide you how to setup this project in your local environment.

## Project

The main goal of project is to provide four parameters given a full_visitor_id, i.e, 

- full_visitor_id
- address_changed
- order_placed
- order_delivered
- application_type

The dataset is available in google cloud storage.

## Approach

The dataset provided consists of google analytics data and corresponding transaction data. 
Firstly I tested my core logic on dataproc clusters. Once satisfied, a flask app was built which upon receiving request, triggers a spark job on dataproc cluster and then returns the above response as string.
After this I containerised entire logic using docker. 

### Pyspark
Since the GA data is nested, which requred unnesting, it was necessary to use something that runs in distributed manner. Using pandas or any other technology caused our app to freeze and eventually terminate.

Hence I chose spark to query this data.

### Dataproc
The app was designed keeping scaling in mind. Since our data was intially hosted on GCP storage, dataproc clusters were used to test and deploy the app.

### Docker
One requirement was to create an RestfulAPI for our app. So I used flask to host my app and containerised it using docker. 

The base image use is google/cloud-sdk:slim. It was preffered as it contains google cloud sdk to help submit spark jobs from within our docker containers.





