#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd
from wandb_utils.log_artifact import log_artifact
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

LOCAL_FILE_NAME = "clean_sample.csv"

def go(args):
    logger.info("Starting basic cleaning")
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    min_price = args.min_price
    max_price = args.max_price
    
    logger.info("Downloading input artifact: {}".format(args.input_artifact))
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Cleaning data...")
    df = pd.read_csv(artifact_local_path)
    min_price = args.min_price
    idx = df['price'].between(min_price, max_price)
    
    df = df[idx].copy()
    
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    
    df['last_review'] = pd.to_datetime(df['last_review'])
    logger.debug("Cleaning finished")
    
    logger.info("Storing file on disk..")
    df.to_csv(LOCAL_FILE_NAME, index=False)
    
    logger.info("Uploading file to WandB...")
    
    log_artifact(
        artifact_name=args.output_artifact,
        artifact_type=args.output_type,
        artifact_description=args.output_description,
        filename=LOCAL_FILE_NAME,
        wandb_run=run
        
    )
    logger.info("Basic cleaning finished.")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Apply basic cleaning to an artifact and saves it")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input artifact incl. version e.g. my_artifact:latest",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact e.g. my_output_artifact ",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price to be included (inclusive).",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price to be included in the data set (inclusive).",
        required=True
    )


    args = parser.parse_args()

    go(args)
