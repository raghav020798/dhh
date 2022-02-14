
import pyspark
import json
import logging
from utils import get_flat_df, flatten_array_struct_df, dist
from pyspark.sql.functions import concat, col, max, when,lit, first, array, split

spark = pyspark.sql.SparkSession.builder.enableHiveSupport().getOrCreate()
logger = logging.getLogger(__name__)

def process(visitor_id):
    """
    Main etl pipeline to fetch json response for a visitor
    :visitor_id (String): full_visitor_id as passed by user through api call. 
    :return: Json repsonse string
    """
    
    logger.info('reading source parquet')
    ga = spark.read.parquet('gs://product-analytics-hiring-tests-public/GoogleAnalyticsSample/ga_sessions_export/')
    tr = spark.read.parquet('gs://product-analytics-hiring-tests-public/BackendDataSample/transactionalData')

    logger.info('flattening analytics dataframe')
    flattened_ga = flatten_array_struct_df(ga).where("fullvisitorid='{}'".format(visitor_id))

    processed_ga = flattened_ga.withColumn('session', concat(col('fullvisitorid'), col('visitStartTime'))) \
                           .withColumn('screen', when(col('hit_customDimensions_index')==11, 
                                                      col('hit_customDimensions_value')).otherwise(None)) \
                           .withColumn('lat', when(col('hit_customDimensions_index')==18, 
                                                      col('hit_customDimensions_value')).otherwise(None)) \
                           .withColumn('long', when(col('hit_customDimensions_index')==19, 
                                                      col('hit_customDimensions_value')).otherwise(None)) \
                        .select(['lat','long','hit_hitNumber','session','screen','fullvisitorid',
                                 'operatingSystem','hit_transactionid'])

    logger.info("grouping on session and hit number")
    processed_ga = processed_ga.groupBy(['session','hit_hitNumber']) \
                            .agg(max('hit_transactionId').alias('transactionId'),
                                 max('fullvisitorid').alias('fullvisitorid'),
                                 max('operatingSystem').alias('application_type'),
                                 max('lat').alias('lat'),
                                 max('long').alias('long'),
                                 max('screen').alias('screen'))

    processed_ga = processed_ga.withColumn('screen', when((~col('transactionId').isNull()), 
                                          lit('checkout')).otherwise(col('screen'))) \
                     .withColumn('start_loc', when((col('screen')=='shop_list') | (col('screen')=='home'), 
                                                   concat(col('long'),lit('|'),col('lat')))) \
                     .withColumn('end_loc', when(col('screen')=='checkout', 
                                                   concat(col('long'),lit('|'),col('lat')))) \
                     .where("lat is not null and long is not null") \
                     .select(['fullvisitorid', 'session','transactionId','application_type',
                              'screen','lat','long','start_loc','end_loc'])

    processed_ga = processed_ga.where("screen in ('home','shop_list','checkout')") \
                .groupBy(['session']) \
                        .agg(max('transactionId').alias('transactionId'),
                             max('fullvisitorid').alias('fullvisitorid'),
                             max('application_type').alias('application_type'),
                             first('start_loc',ignorenulls=True).alias('start_loc'),
                             first('end_loc',ignorenulls=True).alias('end_loc'))

    
    logger.info("incorporating distance logic to get address_changed field")
    start_loc = split(processed_ga['start_loc'], '\|')
    end_loc = split(processed_ga['end_loc'], '\|')
    processed_ga = processed_ga.withColumn('order_placed', when((~col('transactionId').isNull()), True).otherwise(False)) \
                   .withColumn('start_long', start_loc.getItem(0)) \
                   .withColumn('start_lat', start_loc.getItem(1)) \
                   .withColumn('end_long', end_loc.getItem(0)) \
                   .withColumn('end_lat', end_loc.getItem(1)) \
                   .withColumn("dist", dist(
                                "start_long", "start_lat",
                                 "end_long", "end_lat"
                            ).alias("dist")) \
                   .withColumn('address_changed', when(col('dist')>2, True).otherwise(False))

    logger.info('joining analytics data with transaction data')
    joined_df = processed_ga.join(tr, tr.frontendOrderId==processed_ga.transactionId,'left') \
                        .select('fullvisitorid',
                               'order_placed',
                               'status_id',
                               'frontendOrderId',
                               'transactionId',
                               'application_type',
                               'address_changed') \
                        .withColumn('order_delivered', 
                                    when((~col('transactionId').isNull()) & (col('status_id')=='24'), 
                                         True).otherwise(False))

    logger.info('making response string')
    df_list_of_jsons = joined_df.toJSON().collect()
    df_list_of_dicts = [json.loads(x) for x in df_list_of_jsons]
    df_json = json.dumps(df_list_of_dicts)

    logger.info("response: ##"+df_json+'##')
    
    return df_list_of_dicts