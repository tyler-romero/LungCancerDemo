import sys,os
import numpy as np
import dicom
import pyodbc
import pickle
import glob
import pandas as pd
from lung_cancer.connection_settings import get_connection_string, TABLE_SCAN_IMAGES
from config_preprocessing import STAGE1_LABELS, STAGE1_FOLDER, IMAGES_FOLDER


def create_table_scan_images(table_name, cursor, connector, drop_table=False):
    query = ""
    if drop_table:
        query += "IF OBJECT_ID(\'" + table_name + "\') IS NOT NULL DROP TABLE " + table_name + " "
    query += "CREATE TABLE " + table_name
    query += """ ( 
    [id] [uniqueidentifier] ROWGUIDCOL NOT NULL UNIQUE,
    [patient_id] varchar(50) not null, 
    [image_file] varbinary(max) filestream null 
    )
    """
    cursor.execute(query)
    connector.commit()

def get_image_paths(path):
    return [os.path.join(path, s) for s in os.listdir(path)]

def generate_insert_query(table_name, patient_id, img_path):
    query = "INSERT INTO " + table_name +\
            "(id, patient_id, image_file)" \
            " VALUES (NEWID(), '{}', (SELECT BulkColumn FROM OPENROWSET(BULK '{}', SINGLE_BLOB) as f));".format(patient_id, img_path)
    print(query)
    return query


if __name__ == "__main__":

    #Create SQL database connection and table
    connection_string = get_connection_string()
    conn = pyodbc.connect(connection_string)
    cur = conn.cursor()

    print("Creating table {}".format(TABLE_SCAN_IMAGES))
    create_table_scan_images(TABLE_SCAN_IMAGES, cur, conn, drop_table=True)

    #Insert all patient images AS FILESTREAMS
    df = pd.read_csv(STAGE1_LABELS)
    print("Inserting {} patient images in the database".format(df.shape[0]))

    for i, patient_id in enumerate(df['id'].tolist()):

        folder = os.path.join(IMAGES_FOLDER, patient_id)
        print("Inserting patient #{} with id: {}".format(i, patient_id))
        paths = get_image_paths(folder)
        for path in paths:
            q_insert = generate_insert_query(TABLE_SCAN_IMAGES, patient_id, path)
            cur.execute(q_insert)
            conn.commit()

    print("Images inserted")
    conn.close()
