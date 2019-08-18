import threading, time

class Scraper(threading.Thread):
    def __init__(self, symbol, source, pipeline, exitFlag):
        super().__init__()
        self.symbol = symbol
        self.source = source
        self.pipeline = pipeline
        self.exitFlag = exitFlag

    def run(self):
        print("> starting "+self.symbol+" thread")
        while not self.exitFlag.isSet():
            self.pipeline.put([1, self.symbol, 120, "10:10:10"])
            time.sleep(1)
        print("> stopping "+self.symbol+" thread")
