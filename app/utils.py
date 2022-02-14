
import pyspark
from pyspark.sql.functions import col, acos, cos, sin, lit, toRadians
from pyspark.sql.column import Column, _to_java_column
sc = pyspark.SparkContext.getOrCreate()


def dist(long_x, lat_x, long_y, lat_y):
    """
    Find distance between two coordinate points
    :param long_x float: point x longitude
    :param long_y float: point y longitude
    :param lat_x float: point x latitude
    :param lat_y float: point y latitude
    :return: distance in km
    """
    return acos(
        sin(toRadians(lat_x)) * sin(toRadians(lat_y)) + 
        cos(toRadians(lat_x)) * cos(toRadians(lat_y)) * 
            cos(toRadians(long_x) - toRadians(long_y))
    ) * lit(6371.0)

def explode_outer(col):
    _explode_outer = sc._jvm.org.apache.spark.sql.functions.explode_outer 
    return Column(_explode_outer(_to_java_column(col)))

def get_flat_df(nested_df):
    """
    Flatten Json file using stack approach and output dataframe having only child nodes or
    array nodes as parent.
    :param nested_df (pyspark.sql.DataFrame): Nested dataframe 
    :return: Flat dataframe
    """
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]
        
        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)

def flatten_array_struct_df(df):
    """
    Accept flat dataframe read from JSON file, explode array nodes, flatten corresponding child nodes and return subset dataframe

    :param df:  (pyspark.sql.DataFrame)  Input Dataframe
    :param feed: (datalake_load.specification.feed.Feed)  Feed object
    :return: Dataframe with subset of columns specified in the feed object
    """
    array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
    
    while len(array_cols) > 0:
        
        for array_col in array_cols:           
            
            df = df.withColumn(array_col, explode_outer(col(array_col)))
            
        df = get_flat_df(df)
        
        array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]

    for name in df.schema.names:
        df = df.withColumnRenamed(name, name.replace('-', '_'))
    return df 

