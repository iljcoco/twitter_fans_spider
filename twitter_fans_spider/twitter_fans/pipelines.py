# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
import tweepy
import time
import sys
import datetime
import logging
import os
from scrapy.exceptions import CloseSpider

MAX_FETCH_FANS = 200000
# MAX_FETCH_FANS = 10
'''
    DDL:
        CREATE TABLE `t_twitter_user_1` (
          `id` bigint(20) NOT NULL AUTO_INCREMENT,
          `twitter_id` bigint(20) NOT NULL,
          `status` int(10) NOT NULL DEFAULT '0' COMMENT '0-crawled; 1-image downloaded; ',
          `insert_time` datetime DEFAULT NULL,
          `region` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL,
          `name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `picture` varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL,
          `m_picture` varchar(1000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `ext` varchar(4096) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `fans_of` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `twitter_id` (`twitter_id`)
        ) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
'''

def initLogging(logFileName):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s-%(levelname)s-%(message)s',
        datefmt='%y-%m-%d %H:%M',
        filename=logFileName,
        filemode='w'
    );

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


class TwitterFansPipeline(object):
    def __init__(self, host, database, user, password, port):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.count=0

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host='localhost',
            database='mysql',
            user='root',
            password='root123456',
            port=3306,
        )

    def open_spider(self, spider):
        self.db = pymysql.connect(self.host, self.user, self.password, self.database, charset='utf8',
                                  port=self.port)
        self.cursor = self.db.cursor()

    def close_spider(self, spider):
        self.db.close()

    def process_item(self, item, spider):
        start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('start_time=%s'%start_time)
        # print('twitter name=%s'%item['name'])
        try:
            dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = "insert into t_twitter_user_1 (twitter_id,insert_time,region,name,picture,fans_of) values(%s,'%s','%s','%s','%s','%s')" % \
                  (item['id'], dt, 'RU', item['name'], item['icon'], 'MariaSharapova')
            self.cursor.execute(sql)
            self.db.commit()
            self.count=self.count+1
        except pymysql.err.InternalError as e:
            code, msg = e.args
            logging.error('error is:%s' % msg)
        except pymysql.err.ProgrammingError as e:
            code, msg = e.args
            logging.error('error is:%s' % msg)
        except pymysql.err.IntegrityError as e:
            code, msg = e.args
            logging.error('error is:%s' % msg)
        except pymysql.err.NotSupportedError as e:
            code, msg = e.args
            logging.error('error is:%s' % msg)
        except pymysql.err.OperationalError as e:
            code, msg = e.args
            logging.error('error is:%s' % msg)
        except pymysql.err as e:
            logging.error('error is:%s' % e)

        if self.count > MAX_FETCH_FANS:
            print('fetch max count=%s' % self.count)
            end_time_1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('end_time l in is:%s'%end_time_1)
            self.close_spider('crawl_twitter_spider')
            # raise CloseSpider('fetch max count=%s'%self.count)

        return item