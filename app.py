import csv, time, yaml
import threading, queue
import signal
from scraper import Scraper
from datastoreOrchestrator import DatastoreOrchestror

def stopExtraction(sig, frame):
    print("> Stopping Extraction")

def getDatabaseParams():
    with open("database.yaml", 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

def app():
    
    sources = []
    with open('sources.csv', 'r') as csvfile: 
        csvreader = csv.reader(csvfile) 
        for row in csvreader: 
            sources.append(row)
    
    pipeline = queue.Queue()
    exitFlag = threading.Event()

    datastore = DatastoreOrchestror(pipeline, exitFlag, getDatabaseParams())
    datastore.start()
    
    workerThreads = []
    for s in sources:   
        t = Scraper(s[0],s[1], pipeline, exitFlag)
        t.start()
        workerThreads.append(t)

    signal.signal(signal.SIGINT, stopExtraction)
    signal.pause()

    exitFlag.set()

    for t in workerThreads:
        t.join()    
    datastore.join()

if __name__ == "__main__":
    app()

"""
Threads can be run by:
    1. passing a callable object to threading constructor
    2. overriding run() method of threading class

- Thread activity is started by calling start() which triggers the run() in a separate thread
- thread is 'alive' which run() is being executed; is_alive() can be used to check state.
- When T1 calls T2's join(), T1 will wait until T2 is terminated
- If you want your thread to stop gracefully, use non-daemon threads and make use of 'Event' signalling

"""