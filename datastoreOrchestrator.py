import threading, time
import psycopg2
import io, csv

class DatastoreOrchestror(threading.Thread):
    def __init__(self, pipeline, exitFlag):
        super().__init__()
        self.pipeline = pipeline
        self.exitFlag = exitFlag
        self.batch = []
        self.conn = psycopg2.connect("dbname=trade user=postgres password=Stonebraker host=localhost port=6001")

    def run(self):
        print("> starting datastore orchestrator thread")
        while not self.exitFlag.isSet():
            while not self.pipeline.empty():
                if len(self.batch) == 10:

                    print("flush batch")
                    print(self.batch)
                    cur = self.conn.cursor()
                    
                    csv.writer(source, delimiter='\t')
                    
                    # source = ""
                    # for r in self.batch:
                    #     for c in r:
                    #         source += str(c) + "\t"
                    #     source += "\n"
                            
                    print(source)
                    cur.copy_from(io.StringIO(source), 'harvest.ticker', columns=('id', 'symbol', 'value', 'timestamp'))
                    print(cur.fetchone())
                    cur.close()
                    self.conn.close()
                    
                    self.batch = []
                else:
                    print(len(self.batch))
                    self.batch.append(self.pipeline.get())
        print("> stopping datastore orchestrator thread")