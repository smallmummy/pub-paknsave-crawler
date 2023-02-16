# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlPaknsaveItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    store_id = scrapy.Field()
    store_name = scrapy.Field()

    l1_url = scrapy.Field()
    l1_name = scrapy.Field()
    l2_url = scrapy.Field()
    l2_name = scrapy.Field()
    l3_url = scrapy.Field()
    l3_name = scrapy.Field()
    total_amt_product = scrapy.Field()
    product_name = scrapy.Field()
    product_spec = scrapy.Field()
    productId = scrapy.Field()
    productVariants = scrapy.Field()
    restricted = scrapy.Field()
    PriceMode = scrapy.Field()
    PricePerItem = scrapy.Field()
    HasMultiBuyDeal = scrapy.Field()
    MultiBuyDeal = scrapy.Field()
    PricePerBaseUnitText = scrapy.Field()
    MultiBuyBasePrice = scrapy.Field()
    MultiBuyPrice = scrapy.Field()
    MultiBuyQuantity = scrapy.Field()
    product_img = scrapy.Field()
    product_banner = scrapy.Field()
