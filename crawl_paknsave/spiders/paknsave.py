# -*- coding: utf-8 -*-
import scrapy
import json
import traceback


class PaknsaveSpider(scrapy.Spider):
    name = 'paknsave'
    output_path = "./"
    output_name = "StoreList.json"
    output_file = output_path + output_name

    def __init__(self, output_path=None, output_name=None):
        if output_name is not None:
            self.output_name = output_name
        if output_path is not None:
            self.output_path = output_path

        self.output_file = self.output_path + self.output_name

    def start_requests(self):
        try:
            store_url = 'https://www.paknsaveonline.co.nz/CommonApi/Store/GetStoreList'

            yield scrapy.Request(url=store_url, callback=self.parse_store_info)
        except Exception:
            self.logger.error(traceback.format_exc())

    def parse_store_info(self, response):
        try:
            stores = []
            res = json.loads(response.body)
            for i in range(len(res["stores"])):
                store = {}
                store["store_id"] = res["stores"][i]["id"]
                store["store_name"] = res["stores"][i]["name"]
                stores.append(store)

            self.logger.info(
                "get {0} stores,the list was put into {1}"
                .format(i+1, self.output_file))
            return stores
        except Exception:
            self.logger.error(traceback.format_exc())
