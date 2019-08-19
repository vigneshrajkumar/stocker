import threading, time
import psycopg2
import io, csv

class DatastoreOrchestror(threading.Thread):
    def __init__(self, pipeline, exitFlag, dbParams):
        super().__init__()
        self.pipeline = pipeline
        self.exitFlag = exitFlag
        self.conn = psycopg2.connect(host=dbParams['host'], port=dbParams['port'], user=dbParams['username'], password=dbParams['password'], dbname=dbParams['database'])

    def run(self):
        print("> starting datastore orchestrator thread")
    
        while not self.exitFlag.isSet():
            while not self.pipeline.empty():
                data = self.pipeline.get()
                cur = self.conn.cursor()
                cur.execute("INSERT INTO harvest.ticker (id, symbol, value, timestamp) VALUES (%(id)s, %(symbol)s, %(value)s, %(ts)s);", {
                   'id': data[0],
                   'symbol': data[1],
                   'value': data[2],
                   'ts': data[3]
                   })
                self.conn.commit()

        cur.close()
        self.conn.close()
        print("> stopping datastore orchestrator thread")