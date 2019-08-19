import threading, time, requests, datetime
from bs4 import BeautifulSoup

class Scraper(threading.Thread):
    def __init__(self, symbol, source, pipeline, exitFlag):
        super().__init__()
        self.symbol = symbol
        self.source = source
        self.pipeline = pipeline
        self.exitFlag = exitFlag

    def run(self):
        print("> starting "+self.symbol+" thread")
        counter = 0
        while not self.exitFlag.isSet():
            print("GET " + self.symbol)
            soup = BeautifulSoup(requests.get(self.source).content, 'html.parser')
            self.pipeline.put([1, self.symbol, soup.find('div', {'id': 'nseTradeprice'}).get_text(), datetime.datetime.now()])
        print("> stopping "+self.symbol+" thread")
