# -*- coding: utf-8 -*-
# @Time   : 19/8/26 下午11:41
# @Author : Vincent
# @Desc : ==============================================
# Life is Short I Use Python!!!                      ===
# If this runs wrong,don't ask me,I don't know why;  ===
# If this runs right,thank god,and I don't know why. ===
# Maybe the answer,my friend,is blowing in the wind. ===
# ======================================================
# @Project : crawl_paknsave
# @FileName: run.py.py
# @Software: PyCharm

import os
import zipfile
import sys
import json
import datetime
import logging
import traceback
import argparse
import time
import boto3
import pytz
import gzip
from botocore.exceptions import ClientError


def store_crawling(logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("begin to crawl store list...")

    os.system(
        'scrapy crawl paknsave -a output_path="{0}" -a output_name="{1}"'
        .format(paknsave_output_path, paknsave_output_name)
    )

    logger.info("crawling store list finish!")

    if not os.path.exists(paknsave_output_file):
        logger.warning(
            "the output file:{0} from 1st crawler is not exist, please check!"
            .format(paknsave_output_file)
        )
        exit()

    logger.info(
        "store list was stored in file:{0}"
        .format(paknsave_output_file)
    )


def prod_crawling(infile, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("begin to crawl each store....")
    try:
        with open(infile) as f:
            for line in f:
                try:
                    line = json.loads(line)
                except Exception:
                    logger.error(
                        f"the content in the infile {infile} should be JSON,please check!"
                    )
                    logger.error(traceback.format_exc())

                # output_name = "{0}_{1}_product.json".format(line["store_id"], cur_date)
                output_name = "{0}_{1}".format(line["store_id"], cur_date)

                logger.info(
                    "begin to crawl {0}(store_id:{1}), out={2}"
                    .format(line["store_name"], line["store_id"], output_name)
                )

                os.system(
                    'scrapy crawl crawl_main -a store_id="{0}" -a store_name="{1}" -a output_path="{2}" -a output_name="{3}"'
                    .format(
                        line["store_id"],
                        line["store_name"],
                        paknsave_output_path,
                        output_name
                    )
                )

                logger.info("crawling {0} finish!".format(line["store_name"]))

        f = open(paknsave_output_path+"dummy_{0}.done".format(cur_date), "w")
        f.close()
    except Exception:
        logger.error(traceback.format_exc())

    logger.info("all stores were crawled, quit now!")


def get_para():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--mode', default='all',
        help='which data you wanna crawl: store_only;prod_only;all'
    )
    parser.add_argument(
        '--store_file', default='',
        help='if the mode is prod_only, you need specify the value which is store list as input'
    )
    return parser.parse_args()


def set_logger():
    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)
    cur_datetime = time.strftime("%Y%m%d", time.localtime())
    fh = logging.FileHandler(f'./log/run_{cur_datetime}.log')
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    fh_std = logging.StreamHandler(sys.stdout)
    fh_std.setLevel(logging.INFO)
    stf_formatter = logging.Formatter("%(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh_std.setFormatter(stf_formatter)
    logger.addHandler(fh)
    logger.addHandler(fh_std)

    return logger


def check_folder():
    if not os.path.exists("./log/"):
        os.makedirs(r"./log/")

    if not os.path.exists("./output/"):
        os.makedirs(r"./output/")


def _compress_file(zipfilename, dirname, zipmode=zipfile.ZIP_DEFLATED, withPath=True, removezip=False):
    '''compress a single file or all files under a specific directory

    Keywords Aguments:
        zipfilename -- the filename of output zip file
        dirname -- single file or a specific directory
        zipmode -- zipfile.ZIP_DEFLATED for compress using zip
                -- zipfile.ZIP_STORED for tar the archive without compress
        withPath-- if True, compress archive with path
        removezip--[BE CAREFUL]if True, the ori archive will be delete
    Return:
    '''
    if os.path.isfile(dirname):
        with zipfile.ZipFile(zipfilename, 'w', zipmode) as z:
            if withPath:
                z.write(dirname)
            else:
                z.write(dirname, os.path.basename(dirname))
            if removezip:
                os.remove(dirname)
    else:
        with zipfile.ZipFile(zipfilename, 'w', zipmode) as z:
            for root, dirs, files in os.walk(dirname):
                for single_file in files:
                    if single_file != zipfilename:
                        filepath = os.path.join(root, single_file)
                        if withPath:
                            z.write(filepath)
                        else:
                            z.write(filepath, single_file)
                        if removezip:
                            os.remove(dirname)


def _compress_single_file_gz(zipfilename, dirname, compresslevel=1, removezip=False):
    '''compress a single file to gzip File

    Keywords Aguments:
        zipfilename -- the filename of output zip file
        dirname -- single file or a specific directory
        compresslevel -- how heavy it compress
        removezip--[BE CAREFUL]if True, the ori archive will be delete
    Return:
        if dirname is a directory or not sinlge file, return False
    '''
    if os.path.isfile(dirname):
        with gzip.open(zipfilename, 'wb', compresslevel=compresslevel) as z:
            with open(dirname, "rb") as read_file:
                z.write(read_file.read())
            if removezip:
                os.remove(dirname)
    else:
        return False


def put_object(dest_bucket_name, dest_object_name, src_data):
    """Add an object to an Amazon S3 bucket

    The src_data argument must be of type bytes or a string that references
    a file specification.

    :param dest_bucket_name: string
    :param dest_object_name: string
    :param src_data: bytes of data or string reference to file spec
    :return: True if src_data was added to dest_bucket/dest_object, otherwise
    False
    """

    # Construct Body= parameter
    if isinstance(src_data, bytes):
        object_data = src_data
    elif isinstance(src_data, str):
        try:
            object_data = open(src_data, 'rb')
            # possible FileNotFoundError/IOError exception
        except Exception as e:
            logging.error(e)
            return False
    else:
        logging.error('Type of ' + str(type(src_data)) +
                      ' for the argument \'src_data\' is not supported.')
        return False

    # Put the object
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=dest_bucket_name, Key=dest_object_name, Body=object_data)
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
        logging.error(e)
        return False
    finally:
        if isinstance(src_data, str):
            object_data.close()
    return True


def is_dst(dt=None, timezone="UTC"):
    if dt is None:
        dt = datetime.datetime.utcnow()
    timezone = pytz.timezone(timezone)
    timezone_aware_date = timezone.localize(dt, is_dst=None)
    return timezone_aware_date.tzinfo._dst.seconds != 0


if __name__ == "__main__":
    try:
        check_folder()
        logger = set_logger()

        logger.info(f'now is {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

        utc_dt = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        if is_dst(timezone="Pacific/Auckland"):
            nz_dt = utc_dt.astimezone(datetime.timezone(datetime.timedelta(hours=13)))
        else:
            nz_dt = utc_dt.astimezone(datetime.timezone(datetime.timedelta(hours=12)))
        logger.info(f'NZ is {nz_dt.strftime("%Y-%m-%d %H:%M:%S")}')

        cur_date = nz_dt.strftime("%Y-%m-%d")
        paknsave_output_path = "./output/"
        paknsave_log_path = "./log/"

        ziplogfile = f"crawler_log_{cur_date}.zip"
        bucket_name = "paknsave-crawler"
        obj_prefix = "output/"
        log_obj_prefix = "log/"

        paknsave_output_name = "StoreList_{0}.json".format(cur_date)
        paknsave_output_file = paknsave_output_path + paknsave_output_name

        args = get_para()
        if args.mode == "all":
            store_crawling(logger=logger)
            prod_crawling(paknsave_output_path+paknsave_output_name, logger=logger)
        elif args.mode == "store_only":
            store_crawling(logger=logger)
        elif args.mode == "prod_only":
            prod_crawling(args.store_file, logger=logger)
        else:
            logger.error("invalid value for mode,please use -h to check")
            exit()

        # upload log to S3
        _compress_file(ziplogfile, paknsave_log_path, zipmode=zipfile.ZIP_DEFLATED, withPath=False, removezip=False)
        success = put_object(
            bucket_name, log_obj_prefix + ziplogfile, ziplogfile)
        if success:
            logger.info(f'Added {log_obj_prefix + ziplogfile} to {bucket_name}')

    except Exception:
        logger.error(traceback.format_exc())
