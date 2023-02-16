# -*- coding: utf-8 -*-
import scrapy
import json
from crawl_paknsave.items import CrawlPaknsaveItem
import copy
import traceback
import logging
import time, sys


class CrawlMainSpider(scrapy.Spider):
    name = 'crawl_main'
    output_path = "./"
    output_name = 'default'
    postfix_product = ".pro.json"
    postfix_store = ".store.json"
    XPATHisWrong = False

    #logger = logging.getLogger("main")

    def __init__(
        self, store_id, store_name, output_path=None, output_name=None
    ):

        self.store_id = store_id
        self.store_name = store_name

        if output_name is not None:
            self.output_name = output_name
        if output_path is not None:
            self.output_path = output_path

        self.output_product_file = \
            self.output_path + self.output_name + self.postfix_product

        self.output_store_file = \
            self.output_path + self.output_name + self.postfix_store

    def start_requests(self):
        try:
            item = CrawlPaknsaveItem()
            item["store_id"] = self.store_id
            item["store_name"] = self.store_name

            self.logger.info(
                "got store_id={0},store_name={1},start to crawl....".
                format(item["store_id"], item["store_name"]))

            yield scrapy.Request(
                url="https://www.paknsaveonline.co.nz",
                callback=self.parse_dummy,
                meta={'item': copy.deepcopy(item)}
            )
        except Exception:
            self.logger.error(traceback.format_exc())
            yield item

    def parse_dummy(self, response):
        try:
            item = response.meta['item']
            self.logger.debug("login the store....")
            yield scrapy.Request(
                url="https://www.paknsaveonline.co.nz/CommonApi/Store/ChangeStore?"
                    "storeId=%s" % item["store_id"],
                callback=self.parse_ChangeStore, meta={'item': copy.deepcopy(item)}
            )
        except Exception:
            self.logger.error(traceback.format_exc())
            yield item

    def parse_ChangeStore(self, response):
        try:
            item = response.meta['item']
            self.logger.info("fetching all menu in the store")
            yield scrapy.Request(
                url="https://www.paknsaveonline.co.nz/CommonApi/Navigation/MegaMenu?"
                    "storeId=%s" % item["store_id"],
                callback=self.parse_GetMenu, meta={'item': copy.deepcopy(item)}
            )
        except Exception:
            self.logger.error(traceback.format_exc())
            yield item

    def parse_GetMenu(self, response):
        try:
            item = response.meta['item']
            res = json.loads(response.body)
            l1 = res["NavigationList"][0]["Children"]

            for index_1, val_1 in enumerate(l1):

                item["l1_url"] = val_1["URL"]
                item["l1_name"] = val_1["ItemName"]

                l2 = val_1["Children"]
                for index_2, val_2 in enumerate(l2):
                    item["l2_url"] = val_2["URL"]
                    item["l2_name"] = val_2["ItemName"]

                    l3 = val_2["Children"]
                    for index_3, val_3 in enumerate(l3):
                        item["l3_url"] = val_3["URL"]
                        item["l3_name"] = val_3["ItemName"]

                        yield scrapy.Request(
                            url=response.urljoin(item["l3_url"]),
                            callback=self.parse_product_init, meta={
                                'item': copy.deepcopy(item)
                            }
                        )

            """
            item["l1_url"] = "fresh-foods-and-bakery"
            item["l1_name"] = "fresh-foods-and-bakery"
            item["l2_url"] = "fruit--vegetables"
            item["l2_name"] = "fruit--vegetables"
            item["l3_url"] = "prepacked-fresh-fruit"
            item["l3_name"] = "prepacked-fresh-fruit"
            yield scrapy.Request(
                url="https://www.paknsaveonline.co.nz/category/"
                    "fresh-foods-and-bakery/seafood/fresh-shellfish",
                callback=self.parse_product_init, meta={
                    'item': copy.deepcopy(item)
                }
            )
            """
        except Exception:
            self.logger.error(traceback.format_exc())
            yield item

    def parse_product_init(self, response):
        self.XPATHisWrong = False

        try:
            self.logger.debug("start to crawl product detail.....")
            item = response.meta['item']

            total_products_xpath = \
                response.xpath("//div[@class='fs-product-filter__item u-color-half-dark-grey u-hide-down-l']/text()") \
                .re(r'of(.*)products')
            if total_products_xpath is None or len(total_products_xpath) == 0:
                raise XPATHException("total_products_xpath is wrong")

            total_products = total_products_xpath[0].strip()
            item["total_amt_product"] = int(total_products)

            all_product_name = response.xpath(
                "//div[@class='fs-product-card__description']/h3[@class='u-p2']/text()") \
                .getall()
            if all_product_name is None or len(all_product_name) == 0:
                raise XPATHException("all_product_name xpath is wrong")

            all_product_spec = response.xpath(
                "//div[@class='fs-product-card__description']/p[@class='u-color-half-dark-grey u-p3']"). \
                re(r'<p.*>(.*)</p>')
            if all_product_spec is None or len(all_product_spec) == 0:
                raise XPATHException("all_product_spec xpath is wrong")

            all_product_img = response.xpath("///div[@class='fs-product-card__product-image']/@style") \
                .re(r'url\(\'(.*)\'')
            if all_product_img is None or len(all_product_img) == 0:
                raise XPATHException("all_product_img xpath is wrong")

            all_product_banner = response.xpath("//div[@class='fs-product-card__badge']/img/@src") \
                .getall()
            if all_product_banner is None or len(all_product_banner) == 0:
                raise XPATHException("all_product_banner xpath is wrong")

            temp_product_detail = response.xpath(
                "///div[@class='js-product-card-footer fs-product-card__footer-container']/@data-options") \
                .getall()
            if temp_product_detail is None or len(temp_product_detail) == 0:
                raise XPATHException("temp_product_detail xpath is wrong")

            for index, name in enumerate(all_product_name):
                item["product_name"] = name.replace("\n", "").replace("\r", "")
                item["product_spec"] = all_product_spec[index]
                item["product_img"] = all_product_img[index]
                item["product_banner"] = all_product_banner[index]

                temp_json = json.loads(temp_product_detail[index])

                item["productId"] = temp_json["productId"]
                item["productVariants"] = str(temp_json["productVariants"])
                item["restricted"] = temp_json["restricted"]

                item["PriceMode"] = temp_json["ProductDetails"]["PriceMode"]
                item["PricePerItem"] = float(temp_json["ProductDetails"]["PricePerItem"])
                item["HasMultiBuyDeal"] = temp_json["ProductDetails"]["HasMultiBuyDeal"]
                item["MultiBuyDeal"] = temp_json["ProductDetails"]["MultiBuyDeal"]
                item["PricePerBaseUnitText"] = temp_json["ProductDetails"]["PricePerBaseUnitText"]
                item["MultiBuyBasePrice"] = temp_json["ProductDetails"]["MultiBuyBasePrice"]
                item["MultiBuyPrice"] = temp_json["ProductDetails"]["MultiBuyPrice"]
                item["MultiBuyQuantity"] = temp_json["ProductDetails"]["MultiBuyQuantity"]

                yield item

            # self.logger.debug("got {0} items in this page".format(index + 1))

            next_page_url = response.xpath(
                "///a[@class='btn btn--primary btn--large fs-pagination__btn fs-pagination__btn--next']/@href") \
                .get()

            if next_page_url is not None:
                self.logger.debug("there is the next page, now forward to the next page....")
                next_page_url = response.urljoin(next_page_url)
                yield scrapy.Request(
                    next_page_url,
                    callback=self.parse_product_init,
                    meta={'item': copy.deepcopy(item)}
                )
            else:
                self.logger.debug("this is the last page, finish crawling in this catalog level")
        except XPATHException:
            self.logger.error("xpath error occurred!!")
            self.logger.error("====================URL================")
            self.logger.error(response.url)
            self.logger.error("====================HEADER================")
            self.logger.error(response.headers)
            self.logger.error("====================CONTENT================")
            self.logger.error(response.body.decode(response.encoding))
            self.XPATHisWrong = True

        except Exception:
            self.logger.error("other error occurred!!")
            self.logger.error(traceback.format_exc())
            self.logger.error("====================URL================")
            self.logger.error(response.url)
            self.logger.error("====================HEADER================")
            self.logger.error(response.headers)
            self.logger.error("====================CONTENT================")
            self.logger.error(response.body.decode(response.encoding))
            #yield item
            self.XPATHisWrong = True



class XPATHException(Exception):
    pass