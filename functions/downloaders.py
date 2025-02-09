import logging
import os
import shutil
import time
import zipfile

import eumdac

from functions.edit_data import uznip_and_edit_data


def download_col_realtime(logger: logging, downloading):
    logger.info("Real-time data download started...")
    consumer_key = "Tkcuid1atG4vK6jkA7114u989vsa"
    consumer_secret = "GYLHLXbGRKC4to7ek02tCpckQ2sa"

    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials)
    datastore = eumdac.DataStore(token)

    collections = {
        "EO:EUM:DAT:0398": "firerisk",
        "EO:EUM:DAT:MSG:CLM": "cloud_mask",
        "EO:EUM:DAT:METOP:IASSND02": "IASI",
        "EO:EUM:DAT:MSG:HRSEVIRI": "SEVIRI",
        "EO:EUM:DAT:0662": "FCI",
        "EO:EUM:DAT:0691": "FCI_LI",
    }

    while downloading:
        for col, folder_name in collections.items():
            selected_collection = datastore.get_collection(col)
            product = selected_collection.search().first()

            collection_folder = os.path.join("MyData", folder_name)
            os.makedirs(collection_folder, exist_ok=True)

            filename = os.path.join(collection_folder, str(product))

            if os.path.exists(filename):
                logger.info(f"File {filename} already exists. Skipping download.")
            else:
                logger.info(f"Downloading product {product}...")
                with product.open() as fsrc, open(filename, mode="wb") as fdst:
                    shutil.copyfileobj(fsrc, fdst)
                    logger.info(f"Download of product {product} finished.")

                uznip_and_edit_data(
                    selected_collection, filename, logger, collection_folder, product
                )

            logger.info("Processing finished. Waiting for the next product...\n")
        time.sleep(60)


def download_col_custom(
    collection, from_date, to_date, btn_stop, logger: logging, downloading, btn_download
):
    logger.info(
        f"Custom data download for collection '{collection}' from {from_date} to {to_date} started..."
    )
    consumer_key = "Tkcuid1atG4vK6jkA7114u989vsa"
    consumer_secret = "GYLHLXbGRKC4to7ek02tCpckQ2sa"

    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials)
    datastore = eumdac.DataStore(token)

    selected_collection = {
        "firerisk": "EO:EUM:DAT:0398",
        "cloud_mask": "EO:EUM:DAT:MSG:CLM",
        "IASI": "EO:EUM:DAT:METOP:IASSND02",
        "SEVIRI": "EO:EUM:DAT:MSG:HRSEVIRI",
        "FCI": "EO:EUM:DAT:0662",
        "FCI_LI": "EO:EUM:DAT:0691",
    }[collection]

    selected_col = datastore.get_collection(selected_collection)
    products = selected_col.search(dtstart=from_date, dtend=to_date)
    for product in products:
        collection_folder = os.path.join("MyData", collection)
        os.makedirs(collection_folder, exist_ok=True)

        filename = os.path.join(collection_folder, str(product))

        if os.path.exists(filename):
            logger.info(f"File {filename} already exists. Skipping download.")
        else:
            logger.info(f"Downloading product {product}...")
            with product.open() as fsrc, open(filename, mode="wb") as fdst:
                shutil.copyfileobj(fsrc, fdst)
                logger.info(f"Download of product {product} finished.")

            uznip_and_edit_data(
                selected_collection, filename, logger, collection_folder, product
            )

        logger.info("Processing finished. Waiting for the next product...\n")
    time.sleep(2)
    downloading = False
    btn_stop.setEnabled(False)
    btn_download.setEnabled(True)
