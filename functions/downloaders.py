import logging
import os
import shutil
import time

import eumdac

from functions.edit_data import uznip_and_edit_data
from settings import CONSUMER_KEY, CONSUMER_SECRET, DATA_DOWNLOADER_FOLDER
from functions.handle_files import collection_folder_to_clear, load_downloaded_files, is_file_downloaded, save_downloaded_file

def connect_to_datastore():
    credentials = (CONSUMER_KEY, CONSUMER_SECRET)
    token = eumdac.AccessToken(credentials)
    return eumdac.DataStore(token)

def download_col_realtime(logger: logging, downloading, selected_folder: str):
    logger.info("Real-time data download started...")

    datastore = connect_to_datastore()

    collections = {
        "EO:EUM:DAT:0398": "firerisk",
        "EO:EUM:DAT:MSG:CLM": "cloud_mask",
        "EO:EUM:DAT:METOP:IASSND02": "IASI",
        "EO:EUM:DAT:MSG:HRSEVIRI": "SEVIRI",
        "EO:EUM:DAT:0662": "FCI",
        "EO:EUM:DAT:0691": "FCI_LI",
    }

    downloaded_files = load_downloaded_files()

    while downloading:
        for col, folder_name in collections.items():
            collection_folder = os.path.join(DATA_DOWNLOADER_FOLDER, folder_name)

            collection_folder_to_clear(col, collection_folder)

            selected_collection = datastore.get_collection(col)
            product = selected_collection.search().first()

            if not product:
                logger.info(f"No product found for {folder_name}. Skipping.")
                continue

            filename = os.path.join(collection_folder, str(product))

            if is_file_downloaded(filename, downloaded_files):
                logger.info(f"File {filename} already downloaded. Skipping.")
                continue  

            logger.info(f"Downloading product {product}...")
            with product.open() as fsrc, open(filename, mode="wb") as fdst:
                shutil.copyfileobj(fsrc, fdst)
                logger.info(f"Download of product {product} finished.")

            uznip_and_edit_data(selected_collection, filename, logger, collection_folder, product, selected_folder)
            save_downloaded_file(filename) 

            logger.info("Processing finished. Waiting for the next product...\n")

        time.sleep(60)

def download_col_custom(collection, from_date, to_date, btn_stop, logger: logging, downloading, btn_download):
    logger.info(f"Custom data download for collection '{collection}' from {from_date} to {to_date} started...")

    datastore = connect_to_datastore()

    collection_map = {
        "firerisk": "EO:EUM:DAT:0398",
        "cloud_mask": "EO:EUM:DAT:MSG:CLM",
        "IASI": "EO:EUM:DAT:METOP:IASSND02",
        "SEVIRI": "EO:EUM:DAT:MSG:HRSEVIRI",
        "FCI": "EO:EUM:DAT:0662",
        "FCI_LI": "EO:EUM:DAT:0691",
    }

    selected_collection = collection_map.get(collection)
    if not selected_collection:
        logger.error(f"Invalid collection: {collection}")
        return

    selected_col = datastore.get_collection(selected_collection)
    products = selected_col.search(dtstart=from_date, dtend=to_date)

    downloaded_files = load_downloaded_files()

    for product in products:
        collection_folder = os.path.join(DATA_DOWNLOADER_FOLDER, collection)
        os.makedirs(collection_folder, exist_ok=True)

        collection_folder_to_clear(str(selected_col), collection_folder)

        filename = os.path.join(collection_folder, str(product))

        if is_file_downloaded(filename, downloaded_files):
            logger.info(f"File {filename} already downloaded. Skipping.")
            continue  

        logger.info(f"Downloading product {product}...")
        with product.open() as fsrc, open(filename, mode="wb") as fdst:
            shutil.copyfileobj(fsrc, fdst)
            logger.info(f"Download of product {product} finished.")

        uznip_and_edit_data(selected_collection, filename, logger, collection_folder, product)
        save_downloaded_file(filename) 

        logger.info("Processing finished. Waiting for the next product...\n")

    time.sleep(2)
    downloading = False
    btn_stop.setEnabled(False)
    btn_download.setEnabled(True)
