import os
import numpy as np
import pandas as pd
import pkg_resources
import pickle
import pyodbc
from pandas import DataFrame
from sklearn.metrics import roc_auc_score, roc_curve
import matplotlib.pyplot as plt
import scipy.misc

from revoscalepy import RxSqlServerData, rx_import, rx_read_object, rx_write_object, RxOdbcData, rx_get_var_names
from microsoftml import load_image, resize_image, extract_pixels, featurize_image, rx_featurize


######################################################################

def print_library_version():
    print(os.getcwd())
    version_pandas = pkg_resources.get_distribution("pandas").version
    print("Version pandas: {}".format(version_pandas))


######################################################################
# for feature generation
def get_patients_id(table_name, connection_string):
    query = "SELECT DISTINCT patient_id FROM {}".format(table_name)
    sqldata = RxSqlServerData(sql_query=query, connection_string=connection_string)
    data = rx_import(sqldata)
    return data["patient_id"].tolist()


def save_image(scans):
    print(scans.shape)
    path = "C:/Users/t-tyrome/Documents/Internship/sql_python_deep_learning/images/scan.png"
    scipy.misc.imsave(path, scans[0,:,:,:].squeeze().transpose(1,2,0))


def gather_image_paths(data, image_folder, n_pics=None):
    total_counter = 0
    data_to_featurize = pd.DataFrame(columns=("patient_id", "image", "label"), index=range(len(data)))
    for i, row in data.iterrows():
        folder = os.path.join(image_folder, row["patient_id"])
        patient_image_paths = [os.path.join(folder, s) for s in os.listdir(folder)]
        for n, image_path in enumerate(patient_image_paths):
            if n == n_pics: break
            data_to_featurize.loc[total_counter] = [row["patient_id"], image_path, row["label"]]
            total_counter += 1
        print("Gathered {} images for patient #{} with id: {}".format(len(patient_image_paths), i, row["patient_id"]))
    print("Gathered {} total images".format(total_counter))
    return data_to_featurize


def compute_features(data, model, compute_context):
    featurized_data = rx_featurize(
        data=data,
        # output_data=features_sql,   # The type (RxSqlServerData) for file is not supported. TODO: use RxSqlServerData for output when its supported
        overwrite=True,
        ml_transforms=[
            load_image(cols={"feature": "image"}),
            resize_image(cols="feature", width=224, height=224),
            extract_pixels(cols="feature"),
            featurize_image(cols="feature", dnn_model=model)
        ],
        ml_transform_vars=["image"],
        compute_context=compute_context,
        report_progress=2,
        verbose=2
    )
    return featurized_data


def insert_model(table_name, connection_string, classifier, name):
    classifier_odbc = RxOdbcData(connection_string, table=table_name)
    rx_write_object(classifier_odbc, key=name, value=classifier, serialize=True, overwrite=True)


def retrieve_model(table_name, connection_string, name):
    classifier_odbc = RxOdbcData(connection_string, table=table_name)
    classifier = rx_read_object(classifier_odbc, key=name, deserialize=True)
    return classifier


def train_test_split(train_id_table, patients_table, p, connection_string):
    pyodbc_cnxn = pyodbc.connect(connection_string)
    pyodbc_cursor = pyodbc_cnxn.cursor()
    pyodbc_cursor.execute("DROP TABLE if exists {};".format(train_id_table))
    pyodbc_cursor.execute("SELECT DISTINCT patient_id INTO {} FROM {} WHERE ABS(CAST(BINARY_CHECKSUM(idx, NEWID()) as int)) % 100 < {} ;".format(p, train_id_table, patients_table))
    pyodbc_cursor.close()
    pyodbc_cnxn.commit()
    pyodbc_cnxn.close()


def average_pool(labels, featurized_data):
    features = []
    for i, row in labels.iterrows():
        patient = featurized_data[featurized_data["patient_id"] == row.patient_id]
        avg_pool_features = patient.drop(["image", "label", "patient_id"], axis=1).mean(axis=0).tolist()  # Todo: do list concat in the pandas way
        features.append(avg_pool_features)
    col_names = ["f" + str(i) for i in range(len(features[0]))]
    features = pd.DataFrame(features, columns=col_names, index=labels.index.values)
    pooled_data = pd.concat([labels, features], axis=1)
    return pooled_data


#########################################################################
#for scoring and evaluating
def get_patient_id_from_index(table_name, connection_string, patient_index):
    patients = get_patients_id(table_name, connection_string)  #FIXME: this could be faster with a new table with (idx, id)
    return patients[patient_index]

def create_formula(sql_data):
    features_all = rx_get_var_names(sql_data)
    features_to_remove = ["label", "patient_id"]
    training_features = [x for x in features_all if x not in features_to_remove]
    formula = "label ~ " + " + ".join(training_features)
    return formula

def roc(y, y_hat):
    print("ROC AUC: ", roc_auc_score(y, y_hat))
    fpr, tpr, thresholds = roc_curve(y, y_hat)
    plt.figure()
    plt.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve')
    plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic example')
    plt.legend(loc="lower right")
    plt.show()

#########################################################################
# for API
#code from https://github.com/miguelgfierro/codebase/blob/master/python/database/sql_server/select_values.py
def select_entry_where_column_equals_value(table_name, connection_string, column_name, value):
    query = "SELECT TOP (1) * FROM {} WHERE {} = '{}'".format(table_name, column_name, value)
    print(query)
    query_sql = RxSqlServerData(sql_query=query, connection_string=connection_string)
    data = rx_import(query_sql)
    return data

def select_top_value_of_column(table_name, connection_string, column_name):
    query = "SELECT TOP(1) " + column_name + " FROM " + table_name
    print(query)
    query_sql = RxSqlServerData(sql_query=query, connection_string=connection_string)
    data = rx_import(query_sql)
    print(data)
    return data.iloc[0, 0]

def test_func():
    print("hello world")