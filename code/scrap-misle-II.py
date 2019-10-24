import scrapy
import time
import json
import logging
import pandas as pd
from scrapy.crawler import CrawlerProcess
from bs4 import BeautifulSoup

#activity_list = ['3673761', '3662467', '3669435', '3662636', '3659777', '3664756', '3663135', '3662547']
activity_data = pd.read_excel('../data/misle/MISLE Incident Investigations DT.xlsx')
scraped_briefs_data = pd.read_csv('../data/misle/scrape/misle-scraped-brief.csv')

activity_list = activity_data['Activity ID'].tolist()
scraped_activity_list = scraped_briefs_data.activity_id.tolist()

not_scraped = []
for activity_id in activity_list:
    if activity_id not in scraped_activity_list:
        not_scraped.append(activity_id)



def getData(cssID, soup):
    data = soup.find(id=cssID)
    if(data is not None):
        return data.text #to extract the text without html tags
    else:
        return ''
    
briefs = []

class MISLEViewStateSpider(scrapy.Spider):
    name = 'misle-viewstate'
    start_urls = ['https://cgmix.uscg.mil/IIR/IIRSearch.aspx']
    download_delay = 1.5
    
    def __init__(self, activity_id=None):
        self.activity_id = activity_id
    
    def parse(self, response):
        yield scrapy.FormRequest('https://cgmix.uscg.mil/IIR/IIRSearch.aspx',
                                 formdata={'__EVENTVALIDATION': response.css('input#__EVENTVALIDATION::attr(value)'
                                                                      ).extract_first(),
                                           'TextBoxActivityNumber': self.activity_id,
                                           'DropDownListVesselService':'ALL',
                                           'TextBoxFromDate':'01/01/2010',
                                           'TextBoxToDate':'10/16/2019',
                                           'ButtonSearch':'Search',
                                           '__VIEWSTATE': response.css('input#__VIEWSTATE::attr(value)'
                                                                      ).extract_first()
                                          },
                                 callback=self.parse_activity)

    def parse_activity(self, response):
        yield scrapy.FormRequest('https://cgmix.uscg.mil/IIR/IIRSearch.aspx',
                                 formdata={'__EVENTVALIDATION': response.css('input#__EVENTVALIDATION::attr(value)'
                                                                      ).extract_first(),
                                           '__VIEWSTATEGENERATOR': response.css('input#__VIEWSTATEGENERATOR::attr(value)'
                                                                      ).extract_first(),
                                           '__EVENTTARGET':'GridViewIIR$ctl02$ReportButton',
                                           '__VIEWSTATE': response.css('input#__VIEWSTATE::attr(value)'
                                                                      ).extract_first()
                                          },
                                 callback=self.parse_results)

    def parse_results(self, response):
        soup = BeautifulSoup(response.body, 'html.parser')
        brief_result = {
            'activity_id': soup.find(id='LabelActivityNumber').text,
            'incident_brief': soup.find(id='LabelIncidentBrief').text
        }
        
        yield brief_result
        
process = CrawlerProcess(settings={
    'FEED_FORMAT':'csv',
    'FEED_URI': '../data/misle/scrape/misle-scraped-brief-II.csv',
    'LOG_LEVEL': logging.WARNING,
})

for i in range(len(not_scraped)):
    if i >= 1100 and i < 1200:
        time.sleep(5)
        process.crawl(MISLEViewStateSpider, str(not_scraped[i]))
    
process.start() # the script will block here until the crawling is finished