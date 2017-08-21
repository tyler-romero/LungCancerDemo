import os
import pandas as pd
from revoscalepy import rx_import, RxSqlServerData, rx_data_step, RxInSqlServer, RxLocalSeq, rx_set_compute_context

from lung_cancer.connection_settings import get_integrated_authentication_connection_string, TABLE_LABELS, TABLE_FEATURES, TABLE_PATIENTS, TABLE_TRAIN_ID, MICROSOFTML_MODEL_NAME, IMAGES_FOLDER
from lung_cancer.lung_cancer_utils import compute_features, gather_image_paths, train_test_split, average_pool

print("Starting routine")

# Connect to SQL Server
connection_string = get_integrated_authentication_connection_string()
sql = RxInSqlServer(connection_string=connection_string)
local = RxLocalSeq()
rx_set_compute_context(local)

# Get paths to filetable images
data = gather_image_paths(IMAGES_FOLDER)

# Featureize images
print("Featurizing Images")
featurized_data = compute_features(data, MICROSOFTML_MODEL_NAME, compute_context=sql)
print(featurized_data)

# Average Pooling
print("Performing Average Pooling")
pooled_data = average_pool(featurized_data)
print(pooled_data)

# Write features to table
tempfeats_sql = RxSqlServerData(table="dbo.tempfeats", connection_string=connection_string)
rx_data_step(input_data=pooled_data, output_file=tempfeats_sql, overwrite=True)

# Perform join with labels
query = """SELECT dbo.tempfeats.*, dbo.labels.label FROM dbo.tempfeats
            INNER JOIN dbo.labels ON dbo.tempfeats.patient_id = dbo.labels.patient_id;"""
tempfeats_sql = RxSqlServerData(sql_query=query, connection_string=connection_string)
features_sql = RxSqlServerData(table="dbo.tempfeats", connection_string=connection_string)
rx_data_step(tempfeats_sql, features_sql, overwrite=True)

print("Routine finished")