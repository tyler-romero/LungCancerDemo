import pandas as pd
from sklearn.decomposition import IncrementalPCA

from lung_cancer.connection_settings import get_connection_string, TABLE_LABELS, TABLE_FEATURES, TABLE_PCA_FEATURES, TABLE_PATIENTS, TABLE_TRAIN_ID, MICROSOFTML_MODEL_NAME, IMAGES_FOLDER
from lung_cancer.lung_cancer_utils import compute_features, gather_image_paths, train_test_split, average_pool

from revoscalepy import rx_import, RxSqlServerData, rx_data_step, RxInSqlServer, RxLocalSeq, rx_set_compute_context

print("Starting routine")

# Connect to SQL Server
connection_string = get_connection_string()
sql = RxInSqlServer(connection_string=connection_string)
local = RxLocalSeq()
rx_set_compute_context(local)


# Get List of Labels and list of patients
print("Gathering patients and labels")
query = "SELECT patient_id, label FROM {}".format(TABLE_LABELS)
data_sql = RxSqlServerData(sql_query=query, connection_string=connection_string)
data = rx_import(data_sql)

data["label"] = data["label"].astype(bool)


# Prepare list of images
print("Preparing list of images")
n_patients = 50    # How many patients do we featurize images for?
data = data.head(n_patients)
data_to_featurize = gather_image_paths(data, IMAGES_FOLDER, n_pics=2)


# Featureize images and save to table
print("Featurizing Images")
featurized_data = compute_features(data_to_featurize, MICROSOFTML_MODEL_NAME, compute_context=local)


# Average Pooling
print("Performing Average Pooling")
pooled_data = average_pool(data, featurized_data)
features_sql = RxSqlServerData(table=TABLE_FEATURES, connection_string=connection_string)
rx_data_step(input_data=pooled_data, output_file=features_sql, overwrite=True)


# Train Test Split
resample = False
if resample:
    print("Performing Train Test Split")
    p = 80
    train_test_split(TABLE_TRAIN_ID, TABLE_PATIENTS, p, connection_string=connection_string)


# Perform PCA transform
print("Performing PCA transform")
n = min(485, n_patients)    # 485 features is the most that can be handled right now
pca = IncrementalPCA(n_components=n, whiten=True)


# def fit_pca(dataset, context):
#     #print(dataset)
#     datas = pd.DataFrame(dataset)
#     feats = datas.drop(["patient_id", "label"], axis=1)
#     #print("features shape: ", feats.shape)
#     #print(feats)
#     pca.partial_fit(feats)
#     return dataset   # Dummy return object


def apply_pca(dataset, context):
    dataset = pd.DataFrame(dataset)
    feats = dataset.drop(["label", "patient_id"], axis=1)
    feats = pca.transform(feats)
    feats = pd.DataFrame(data=feats, index=dataset.index.values, columns=["pc" + str(i) for i in range(feats.shape[1])])
    dataset = pd.concat([dataset[["label", "patient_id"]], feats], axis=1)
    return dataset


query = "SELECT * FROM {} WHERE patient_id IN (SELECT patient_id FROM {})".format(TABLE_FEATURES, TABLE_TRAIN_ID)
train_data_sql = RxSqlServerData(sql_query=query, connection_string=connection_string)
train_data = rx_import(input_data=train_data_sql)
train_data = train_data.drop(["label", "patient_id"], axis=1)
pca.fit(train_data)
# rx_data_step(input_data=train_data_sql, transform_function=fit_pca)


rx_set_compute_context(local)
pca_features_sql = RxSqlServerData(table=TABLE_PCA_FEATURES, connection_string=connection_string)
rx_data_step(input_data=features_sql, output_file=pca_features_sql, overwrite=True, transform_function=apply_pca)

print("Routine finished")