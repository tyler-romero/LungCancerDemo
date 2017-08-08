import os
import dicom
import pandas as pd
import scipy.misc

from config_preprocessing import STAGE1_LABELS, STAGE1_FOLDER
from lung_cancer.connection_settings import IMAGES_FOLDER

def convert_dicom_to_png(dicom_path, image_path):
    slices = [(dicom.read_file(os.path.join(dicom_path, s)),s) for s in os.listdir(dicom_path)]
    for slice, file_name in slices:
        path = os.path.join(image_path, file_name[:-4]+'.png')
        scipy.misc.imsave(path, slice.pixel_array)


if __name__ == "__main__":

    df = pd.read_csv(STAGE1_LABELS)
    print("Converting {} dicoms to pngs".format(df.shape[0]))

    for i, patient_id in enumerate(df['id'].tolist()):

        dicom_folder = os.path.join(STAGE1_FOLDER, patient_id)
        image_folder = os.path.join(IMAGES_FOLDER, patient_id)
        if os.path.exists(image_folder):
            continue

        os.makedirs(image_folder)

        print("Converting patient #{} with id: {}".format(i, patient_id))
        batch = convert_dicom_to_png(dicom_folder, image_folder)

    print("Dicoms converted")
