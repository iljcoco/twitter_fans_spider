import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
import scrapy
from scrapy.http import Request
from scrapy.selector import Selector
from twitter_fans.items import TwitterFansItem
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
import logging
import json
from lxml import etree
import datetime

FANS_MAX_FANS_NUM = 2000

class ScrapyRequestStudy(scrapy.Spider):
    name = "twitter_fans_spider"
    # start_urls =['https://twitter.com/Chris_solis11/followers']
    # start_urls = ['https://twitter.com/5umm/followers']
    # start_urls = ['https://twitter.com/literabook/followers']
    start_urls = ['https://twitter.com/MariaSharapova/followers']
    # start_urls=['https://twitter.com/5umm/followers/users?include_available_features=1&include_entities=1&reset_error_state=false&max_position=1618003791845400616']
    # start_urls = ['https://twitter.com/RealVolya']
    max_position=0


    headers = {
        'Host': 'twitter.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        # "Accept-Encoding":"unicode",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Content-Type": " text/html;charset=utf-8",
        "Referer": "https://twitter.com/MariaSharapova/followers",
    }
    headers1 = {
        'Host': 'twitter.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        # "Accept-Encoding":"unicode",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Content-Type": " text/html;charset=utf-8",
        "Referer": "https://twitter.com/MariaSharapova/followers",
    }
    cookies={
        "personalization_id":"v1_1GhkRrIHbeGLOd28y3MclQ==",
        "guest_id":"v1%3A152763962587374790",
        "_ga":"GA1.2.1945159066.1527639630",
        "ads_prefs":"HBERAAA=",
        "kdt":"vxRcxI7JmLzSgIeCeaiJ3pIjCv6pEqFCQXxFwqc9",
        "remember_checked_on":"1",
        "csrf_same_site_set":"1",
        "eu_cn":"1",
        "lang":"en",
        "csrf_same_site":"1",
        "tfw_exp":"0",
        "_gid":"GA1.2.455968451.1543115553",
        "external_referer":"8e8t2xd8A2w%3D|1|eaCKANC%2Bzih2HIAx%2FHuByf0ZJmqyJFQ%2FIGJuTZSBamdbJic9BgypBqfFgRe7uAoGaz1p8gmjksc%3D",
        "ct0":"331d2728051bf5eeafe066893db94f99",
        "_twitter_sess":"BAh7DCIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCOVo0ypnAToHaWQiJTky%250AZDM4MjgzMjdiM2FkMDE4YTIwMzhlMmYxYWEyYmE1Ogxjc3JmX2lkIiU3MWIw%250AZGU1NTY1YzExYjU4YWQ3OTZhZWM2OTk4YjI5ODofbGFzdF9wYXNzd29yZF9j%250Ab25maXJtYXRpb24iFTE1NDMxMTY5ODA1NTEwMDA6HnBhc3N3b3JkX2NvbmZp%250Acm1hdGlvbl91aWQiFzg4NzQ4MzUwMTgwODcyMTkyMDoJdXNlcmwrCQCAFyOD%250AExoM--6e432c8056b736a63a09ac7ec21c41fe81d29269",
        "twid":"u=872030931799998464",
        "auth_token":"e0c09983579c7222c57ae06708956dec6467c670",
        "mbox":"PC#98cbdde8659140808e7adde08c49add6.22_25#1544780422|session#f9050530540541679da2cf132635270e#1543572682|check#true#1543570882",
        "_gat":"1",
        "app_shell_visited":"1",
        "__utmc":"43838368",
        "__utmz":"43838368.1543682398.2.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)",
        "__utma":"43838368.1945159066.1527639630.1543682398.1543712775.3",
    }

    def start_requests(self):
        self.initLogging('crawl_twitter_spider')
        for url in self.start_urls:
            yield Request(url, headers=self.headers, cookies=self.cookies, callback=self.parseFollowers,errback=self.errback_httpbin,dont_filter=True)

    def parseFollowers(self,response):
        if 'users' not in response.url:
            self.max_position = response.selector.css('div[data-min-position]::attr(data-min-position)').extract_first(
                default='')
            print('max_position in page 1=%s' % self.max_position)
            for ele in response.css('.ProfileCard-content'):
                profile_list=ele.css('img[src*="/profile_images"]::attr(src)').extract()
                if profile_list:
                    protected = ele.css('div[data-protected]::attr(data-protected)').extract_first(default='true')
                    if protected=='false':
                        profile_img=profile_list[0]
                        twitter_id=ele.css('div[data-user-id]::attr(data-user-id)').extract_first(default='')
                        screen_name=ele.css('div[data-screen-name]::attr(data-screen-name)').extract_first(default='')
                        if screen_name:
                            user_info_url='https://twitter.com/'+screen_name
                            self.headers1['Referer']=user_info_url
                            request = scrapy.Request(user_info_url, headers=self.headers, cookies=self.cookies,
                                                    callback=self.parseUser)
                            request.meta['twitter_id'] = twitter_id
                            request.meta['profile_img'] = profile_img
                            request.meta['screen_name'] = screen_name
                            yield request
        else:
            self.max_position=json.loads(response.body)['min_position']
            print('max_position in page 2=%s'%self.max_position)
            html=etree.HTML(json.loads(response.body)['items_html'].encode('utf-8'))
            # print(etree.tostring(html,pretty_print=True))
            # print('type of html=%s'%type(html))
            rep = html.xpath('//div[@class="ProfileCard-content"]')
            for rep_ele in rep:
                img=rep_ele.xpath('./a/img[contains(@src,"/profile_images/")]')
                if img:
                    icon=img[0].xpath('./@src')[0]
                    user_info_list=rep_ele.xpath('.//div[contains(@class,"user-actions")]')
                    if user_info_list:
                        user_info=user_info_list[0]
                        user_id_list=user_info.xpath('./@data-user-id')
                        user_id=user_id_list[0] if user_id_list else ''
                        screen_name_list=user_info.xpath('./@data-screen-name')
                        screen_name=screen_name_list[0] if screen_name_list else ''
                        protected_list=user_info.xpath('./@data-protected')
                        protected=protected_list[0] if protected_list else 'true'
                        if protected=='false' and screen_name and user_id:
                            user_info_url = 'https://twitter.com/' + screen_name
                            self.headers1['Referer'] = user_info_url
                            request = scrapy.Request(user_info_url, headers=self.headers, cookies=self.cookies,
                                                     callback=self.parseUser)
                            request.meta['twitter_id'] = user_id
                            request.meta['profile_img'] = icon
                            request.meta['screen_name'] = screen_name
                            yield request


        if self.max_position:
            next_url='https://twitter.com/MariaSharapova/followers/users?include_available_features=1&include_entities=1&reset_error_state=false&max_position='+self.max_position
            print('next_url=%s'%next_url)
            yield Request(next_url, headers=self.headers, cookies=self.cookies, callback=self.parseFollowers,
                                 errback=self.errback_httpbin,dont_filter=True)


    def parseUser(self,response):
        screen_name=response.meta['screen_name']
        twitter_id=response.meta['twitter_id']
        profile_img=response.meta['profile_img']
        # select=response.selector
        select=response
        # fans_count = select.css('a[data-nav="following"]').css(
        #     'span[class="ProfileNav-value"]::attr(data-count)').extract_first(default='')
        href_value=select.css('a[data-nav="following"]::attr(href)').extract_first(default='')
        if screen_name in href_value:
            fans_count=select.css('a[data-nav="following"]').css(
            'span[class="ProfileNav-value"]::attr(data-count)').extract_first(default='')
            if fans_count:
                if int(fans_count)<FANS_MAX_FANS_NUM:
                    item = TwitterFansItem()
                    item['id']=twitter_id
                    item['name']=screen_name
                    item['icon']=profile_img
                    yield item

    def errback_httpbin(self, failure):
        # log all failures
        logging.error(repr(failure))

        # in case you want to do something special for some errors,
        # you may need the failure's type:

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            logging.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            logging.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            logging.error('TimeoutError on %s', request.url)

    def initLogging(self,logFileName):
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