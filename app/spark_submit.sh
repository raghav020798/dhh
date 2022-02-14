echo "visitor: $1";
export code='main.py'
gcloud dataproc jobs submit pyspark $code \
--cluster 'dhh-cluster' \
--region 'us-central1' \
--properties spark.submit.deployMode=client \
--py-files 'etl.py','utils.py' \
-- --visitor_id  $1
