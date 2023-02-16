# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import hashlib
import traceback
import scrapy
import copy
import datetime
import re
import pandas as pd
import os
import gzip
import boto3
from fastparquet import write
from botocore.exceptions import ClientError


BUCKET_NAME = "paknsave-crawler"

BUCKET_FILE_PREFIX_INPUT_STORE = "output_test/store/"
BUCKET_FILE_PREFIX_INPUT_PORD = "output_test/prod/"

ETL_KEY_PREFIX_STORE = "after_etl_test/store/"
ETL_KEY_PREFIX_PROD = "after_etl_test/prod/"
ETL_KEY_PREFIX_CATALOG = "after_etl_test/catalog/"


def _compress_single_file_gz(
    zipfilename, dirname, compresslevel=1, removezip=False
):
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


class CrawlPaknsavePipeline(object):

    cur_date = datetime.datetime.now().strftime("%Y-%m-%d")
    store_record = dict()

    def put_object(self, spider, dest_bucket_name, dest_object_name, src_data):
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
                spider.logger.error(e)
                return False
        else:
            spider.logger.error(
                'Type of ' + str(type(src_data)) + ' for the argument \'src_data\' is not supported.')
            return False

        # Put the object
        s3 = boto3.client('s3')
        try:
            s3.put_object(
                Bucket=dest_bucket_name, Key=dest_object_name, Body=object_data
            )
        except ClientError as e:
            # AllAccessDisabled error == bucket not found
            # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
            spider.logger.error(e)
            return False
        finally:
            if isinstance(src_data, str):
                object_data.close()
        return True

    def open_spider(self, spider):
        """
        if spider.name == 'paknsave':
            self.file = open('StoreList.json', 'w')
        elif spider.name == 'crawl_main':
            self.file = open('productlist.json', 'w')
        """
        try:
            if spider.name == 'paknsave':
                self.paknsave_file = open(spider.output_file, 'w')
            elif spider.name == 'crawl_main':
                self.output_product_file = open(spider.output_product_file, 'w')
                self.output_store_file = open(spider.output_store_file, 'w')
        except Exception:
            spider.logger.error(
                "There is the fatal error in open file!detail:{0}"
                .format(traceback.format_exc())
            )
            raise scrapy.exceptions.CloseSpider(
                reason='halted by open file error!'
            )

    def close_spider(self, spider):
        try:
            # the process of StoreList.json file
            if spider.name == 'paknsave':
                self.paknsave_file.close()
            elif spider.name == 'crawl_main':
                self.output_product_file.close()
                self.output_store_file.close()

                self.process_StoreFile(spider)
                self.process_ProdFile(spider)

        except Exception:
            spider.logger.error(
                "There is the fatal error in pipeline!detail:{0}"
                .format(traceback.format_exc())
            )

    def get_md5(self, str, spider):
        try:
            m = hashlib.md5()
            m.update(str.encode("utf-8"))
            return m.hexdigest()
        except Exception:
            spider.logger.error(
                "There is the fatal error in pipeline!detail:{0}"
                .format(traceback.format_exc())
            )

    def process_item(self, item, spider):
        try:
            if spider.name == "paknsave":
                line = json.dumps(dict(item)) + "\n"
                self.paknsave_file.write(line)
                return item
            elif spider.name == 'crawl_main':
                item_cp = copy.deepcopy(item)
                item_cp.pop("total_amt_product")
                str = json.dumps(dict(item_cp))
                str_md5 = self.get_md5(str, spider)

                item_new = dict(item_cp)
                item_new["md5"] = str_md5
                item_new["date"] = self.cur_date

                line = json.dumps(item_new)
                self.output_product_file.write(line + "\n")

                if not (item["store_id"]+item["l3_url"] in self.store_record):
                    item_cp = dict()
                    item_cp["store_id"] = item["store_id"]
                    item_cp["store_name"] = item["store_name"]
                    item_cp["l1_url"] = item["l1_url"]
                    item_cp["l1_name"] = item["l1_name"]
                    item_cp["l2_url"] = item["l2_url"]
                    item_cp["l2_name"] = item["l2_name"]
                    item_cp["l3_url"] = item["l3_url"]
                    item_cp["l3_name"] = item["l3_name"]
                    item_cp["total_amt_product"] = item["total_amt_product"]

                    item_cp["date"] = self.cur_date
                    line = json.dumps(item_cp)
                    self.output_store_file.write(line + "\n")
                    self.store_record[item["store_id"]+item["l3_url"]] = ""
                return item
        except Exception:
            spider.logger.error(
                "There is the fatal error in pipeline!detail:{0}"
                .format(traceback.format_exc())
            )

    def process_StoreFile_gzip(self, spider):
        zipfile_store = f"{spider.output_path}crawler_{spider.output_name}{spider.postfix_store}.gz"
        zipfile_store_filename = f"crawler_{spider.output_name}{spider.postfix_store}.gz"

        _compress_single_file_gz(
            zipfile_store, spider.output_store_file, removezip=False
        )

        dest_object_name = \
            BUCKET_FILE_PREFIX_INPUT_STORE + zipfile_store_filename

        self.put_object(
            spider,
            BUCKET_NAME,
            dest_object_name,
            zipfile_store
        )
        spider.logger.info(
            f"file was uploaded to {BUCKET_NAME}/{dest_object_name}"
        )

    def process_StoreFile_parquet(self, spider):
        p_year, p_month, p_day = \
            self.get_partition(spider, spider.output_name.split("_")[1])

        if p_year is None or p_month is None or p_day is None:
            spider.logger.error("get_partition error!")
            return False

        out_parquet_basename = f"crawler_{spider.output_name}.store.parquet"
        out_parquet_filename = f"{spider.output_path}{out_parquet_basename}"
        self.write2parquet(
            spider, spider.output_store_file, out_parquet_filename
        )

        dest_object_name = f"{ETL_KEY_PREFIX_STORE}year={p_year}/month={p_month}/day={p_day}/{out_parquet_basename}"
        self.put_object(
            spider, BUCKET_NAME, dest_object_name, out_parquet_filename
        )
        spider.logger.info(
            f"the Store Files already was put on {BUCKET_NAME}/{dest_object_name}"
        )

    def process_StoreFile(self, spider):
        self.process_StoreFile_gzip(spider)
        self.process_StoreFile_parquet(spider)

        os.remove(spider.output_store_file)

    def process_ProdFile_gzip(self, spider):
        zipfile_product = f"{spider.output_path}crawler_{spider.output_name}{spider.postfix_product}.gz"
        zipfile_product_filename = f"crawler_{spider.output_name}{spider.postfix_product}.gz"

        _compress_single_file_gz(
            zipfile_product, spider.output_product_file, removezip=False
        )

        dest_object_name = \
            BUCKET_FILE_PREFIX_INPUT_PORD + zipfile_product_filename
        self.put_object(
            spider,
            BUCKET_NAME,
            dest_object_name,
            zipfile_product
        )
        spider.logger.info(
            f"file was uploaded to {BUCKET_NAME}/{dest_object_name}"
        )

    def process_ProdFile_parquet(self, spider):
        p_year, p_month, p_day = \
            self.get_partition(spider, spider.output_name.split("_")[1])

        if p_year is None or p_month is None or p_day is None:
            spider.logger.error("get_partition error!")
            return False

        tmp_output_filename_prod = f"{spider.output_path}crawler_{spider.output_name}prod.json"
        tmp_output_filename_catalog = f"{spider.output_path}crawler_{spider.output_name}.catalog.json"
        self.process_file(
            spider, tmp_output_filename_prod, tmp_output_filename_catalog
        )
        spider.logger.info(
            f"got two output file:{tmp_output_filename_prod} & {tmp_output_filename_catalog}"
        )

        # process for catalog separatedly
        out_parquet_basename = f"crawler_{spider.output_name}.catalog.parquet"
        out_parquet_filename = f"{spider.output_path}{out_parquet_basename}"
        self.write2parquet(
            spider, tmp_output_filename_catalog, out_parquet_filename
        )
        spider.logger.info(
            f"the parquet file was put on {out_parquet_filename}"
        )

        dest_object_name = f"{ETL_KEY_PREFIX_CATALOG}year={p_year}/month={p_month}/day={p_day}/{out_parquet_basename}"
        self.put_object(
            spider, BUCKET_NAME, dest_object_name, out_parquet_filename
        )
        spider.logger.info(f"and uploaded to {BUCKET_NAME}/{dest_object_name}")

        # process for prod separatedly
        out_parquet_basename = f"crawler_{spider.output_name}.pro.parquet"
        out_parquet_filename = f"{spider.output_path}{out_parquet_basename}"
        self.write2parquet(
            spider, tmp_output_filename_prod, out_parquet_filename
        )
        spider.logger.info(
            f"the parquet file was put on {out_parquet_filename}"
        )

        dest_object_name = f"{ETL_KEY_PREFIX_PROD}year={p_year}/month={p_month}/day={p_day}/{out_parquet_basename}"
        self.put_object(
            spider, BUCKET_NAME, dest_object_name, out_parquet_filename
        )
        spider.logger.info(f"and uploaded to {BUCKET_NAME}/{dest_object_name}")

    def process_ProdFile(self, spider):
        self.process_ProdFile_gzip(spider)
        self.process_ProdFile_parquet(spider)

        os.remove(spider.output_product_file)

    def write2parquet(self, spider, ori_filename, out_parquet_filename):
        with open(ori_filename, "r") as fr:
            str_content = fr.read()
        df_content = pd.read_json(str_content, orient='records', lines=True)

        if "date" in df_content.columns:
            df_content["date"] = df_content["date"].dt.strftime('%Y-%m-%d')

        spider.logger.info("writing into parquet....")
        write(out_parquet_filename, df_content, compression='GZIP')

    def get_partition(self, spider, object_name):
        return object_name.split("-")

    def process_file(
        self, spider, tmp_output_filename_prod, tmp_output_filename_catalog
    ):

        arr_productId = []

        if os.path.exists(tmp_output_filename_prod):
            os.remove(tmp_output_filename_prod)
        if os.path.exists(tmp_output_filename_catalog):
            os.remove(tmp_output_filename_catalog)

        try:
            fw_prod = open(tmp_output_filename_prod, "a")
            fw_catalog = open(tmp_output_filename_catalog, "a")

            with open(spider.output_product_file, "r") as fr:
                line = fr.readline()
                while line:
                    line_json = json.loads(line)

                    re_mapping = self.mapping_catalog_data(line_json)
                    fw_catalog.write(json.dumps(re_mapping) + "\n")

                    re_mapping = self.mapping_prod_data(
                        spider, line_json, arr_productId
                    )
                    if re_mapping is not None:
                        fw_prod.write(json.dumps(re_mapping) + "\n")

                    line = fr.readline()

        finally:
            fw_prod.close()
            fw_catalog.close()

    def mapping_catalog_data(self, line_json):
        re_mapping = {}
        re_mapping["store_id"] = line_json["store_id"]
        re_mapping["store_name"] = line_json["store_name"]
        re_mapping["l1_url"] = line_json["l1_url"]
        re_mapping["l1_name"] = line_json["l1_name"]
        re_mapping["l2_url"] = line_json["l2_url"]
        re_mapping["l2_name"] = line_json["l2_name"]
        re_mapping["l3_url"] = line_json["l3_url"]
        re_mapping["l3_name"] = line_json["l3_name"]
        re_mapping["productId"] = line_json["productId"]
        re_mapping["date"] = line_json["date"]

        return re_mapping

    def mapping_prod_data(self, spider, line_json, arr_productId):

        productId = line_json["productId"]
        if productId in arr_productId:
            return None
        arr_productId.append(productId)

        re_mapping = {}
        re_mapping["store_id"] = line_json["store_id"]
        re_mapping["store_name"] = line_json["store_name"]
        re_mapping["product_name"] = line_json["product_name"]
        re_mapping["product_spec"] = line_json["product_spec"]
        re_mapping["product_img"] = line_json["product_img"]
        re_mapping["product_banner"] = line_json["product_banner"]
        re_mapping["productId"] = line_json["productId"]
        re_mapping["productVariants"] = line_json["productVariants"]
        re_mapping["restricted"] = line_json["restricted"]
        re_mapping["PriceMode"] = line_json["PriceMode"]
        re_mapping["PricePerItem"] = line_json["PricePerItem"]
        re_mapping["HasMultiBuyDeal"] = line_json["HasMultiBuyDeal"]
        re_mapping["MultiBuyDeal"] = line_json["MultiBuyDeal"]
        re_mapping["PricePerBaseUnitText"] = line_json["PricePerBaseUnitText"]
        re_mapping["MultiBuyBasePrice"] = line_json["MultiBuyBasePrice"]
        re_mapping["MultiBuyPrice"] = line_json["MultiBuyPrice"]
        re_mapping["MultiBuyQuantity"] = line_json["MultiBuyQuantity"]

        str_md5 = self.get_md5(json.dumps(re_mapping), spider)
        re_mapping["md5"] = str_md5

        re_mapping["date"] = line_json["date"]

        return re_mapping
