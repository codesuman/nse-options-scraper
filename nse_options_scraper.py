import requests
import json
import sched, time
from datetime import datetime

from mongoengine import *

class OptionIndices(Document):
    symbol = StringField(required=True, unique=True)
    underlyingValue = FloatField(required=True)
    upcomingExpiryDate = StringField(required=True)
    lastUpdatedTime = StringField(required=True)
    # TODO - Add expiryDates when you want to pre-populate UI with next available set of Expiry Dates. 
    # Till then just display upcomingExpiryDate, that should be good 
    # expiryDates = ListField(StringField(unique=True))

class Options(Document):
    underlying = StringField(required=True)
    strikePrice = IntField(required=True)
    expiryDate = StringField(required=True)
    type = StringField(required=True)

class OptionChainData(Document):
    option_id = ReferenceField(Options, required=True)
    change = FloatField(required=True)
    changeinOpenInterest = FloatField(required=True)
    impliedVolatility = FloatField(required=True)
    lastPrice = FloatField(required=True)
    openInterest = FloatField(required=True)
    pChange = FloatField(required=True)
    pchangeinOpenInterest = FloatField(required=True)
    underlyingValue = FloatField(required=True)
    lastUpdatedTime = StringField(required=True)
    createdAt = DateTimeField(default=datetime.utcnow)

class NSEOptionScraper:
    "This is a NSEOptionScraper class"
    def __init__(self, index):
        print('..........................')
        print(f'NSE Option Scraper constructor : {index}')
        print('..........................')

        # MONGOENGINE - START
        connect('nse_options', host='localhost', port=27017)
        # MONGOENGINE - END

        indices = {'NIFTY', 'BANKNIFTY', 'FINNIFTY'}

        self.index = index
        self.delay = 90

        self.s = sched.scheduler(time.time, time.sleep)

        self.nse_url_oc = 'https://www.nseindia.com/option-chain'
        if self.index in indices:
            self.nse_url_api = 'https://www.nseindia.com/api/option-chain-indices?symbol='
        else:
            print('Setting NSE API URL for EQUITY')
            self.nse_url_api = 'https://www.nseindia.com/api/option-chain-equities?symbol='

        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                'like Gecko) '
                                'Chrome/80.0.3987.149 Safari/537.36',
                'accept-language': 'en,gu;q=0.9,hi;q=0.8', 'accept-encoding': 'gzip, deflate, br'}
        self.session = requests.Session()
        self.cookies = None


    def fetch(self):
        print(f'Fetching OI data for : {self.index}')
        print('START =', datetime.fromtimestamp(time.time()))

        try:
            if self.cookies is None:
                print('FETCHING & SETTING COOKIES...')
                request = self.session.get(self.nse_url_oc, headers=self.headers, timeout=20)
                self.cookies = dict(request.cookies)

            response = self.session.get(self.nse_url_api + self.index, headers=self.headers, timeout=20, cookies=self.cookies)

            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

        dajs = json.loads(response.text)

        # if hasattr(dajs, 'records'):
        lastUpdatedTimeOnNSE = dajs['records']['timestamp']
        lastUpdatedTimeOnDB = None
        ociObjIndex = None

        optionIndex = OptionIndices.objects(symbol=self.index).first()
        print(optionIndex)

        if optionIndex is None:
            print('No data in OCI, CREATING new document.')

            newOptionIndex = OptionIndices(symbol=self.index, underlyingValue=dajs['records']['underlyingValue'], upcomingExpiryDate=dajs['records']['expiryDates'][0], lastUpdatedTime=lastUpdatedTimeOnNSE)
            newOptionIndex.save()

            self.save(dajs)
        else:
            lastUpdatedTimeOnDB = optionIndex['lastUpdatedTime']

            if lastUpdatedTimeOnDB == lastUpdatedTimeOnNSE:
                print('Database is updated, so IGNORING scraped data')
                print('..........................')
            else:
                print('UPDATING LUT on the OCI then save new OI data : ')
                OptionIndices.objects(symbol=self.index).update(underlyingValue=dajs['records']['underlyingValue'], lastUpdatedTime=lastUpdatedTimeOnNSE, upcomingExpiryDate=dajs['records']['expiryDates'][0])

                print('UPDATING LUT completed : ')
                optionIndex = OptionIndices.objects(symbol=self.index).first()
                print(optionIndex.lastUpdatedTime)

                self.save(dajs)

        self.s.enter(self.delay, 1, self.fetch)
        self.s.run()


    def save(self, dajs, processingObject = 'filtered'):
        print(f"SAVE OI CHAIN -> Total docs : {len(dajs[processingObject]['data'])}")

        lastUpdatedTimeOnNSE = dajs['records']['timestamp']

        for scraped_data in dajs[processingObject]['data']:
            self.save_option_chain_data(scraped_data, lastUpdatedTimeOnNSE, 'CE')
            self.save_option_chain_data(scraped_data, lastUpdatedTimeOnNSE, 'PE')
        
        print('************************')
        print(self.index)
        print('************************')

        print('END =', datetime.fromtimestamp(time.time()))
        print('..........................')


    def save_option_chain_data(self, data, lastUpdatedTimeOnNSE, optionType):
        # First check if Option (e.g., NIFTY 15000 CE) is present, else create one.
        # Then use the Option ID to create Option Chain Data

        option = Options.objects(underlying=self.index, strikePrice=data['strikePrice'], expiryDate=data['expiryDate'], type=optionType).first()
        if option is None:
            print('No OPTION, CREATING new document : ')
            print(self.index + ' ' + str(data['strikePrice']) + ' ' + optionType + ' ' + data['expiryDate'])

            newoption = Options(underlying=self.index, strikePrice=data['strikePrice'], expiryDate=data['expiryDate'], type=optionType)
            newoption.save()

            option = newoption

            print('New OPTION CREATED.')
            print(option.id)
            print('.....')

        # Take Option ID and save Option Chain Data
        optionChainData = OptionChainData(option_id=option, change=data[optionType]['change'], changeinOpenInterest=data[optionType]['changeinOpenInterest'], impliedVolatility=data[optionType]['impliedVolatility'], lastPrice=data[optionType]['lastPrice'], openInterest=data[optionType]['openInterest'], pChange=data[optionType]['pChange'], pchangeinOpenInterest=data[optionType]['pchangeinOpenInterest'], underlyingValue=data[optionType]['underlyingValue'], lastUpdatedTime=lastUpdatedTimeOnNSE)
        optionChainData.save()