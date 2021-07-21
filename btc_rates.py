import requests
import psycopg2
from datetime import datetime


class DBConnection(object):
    ## TO-DO: put passwords to env variables 
    def __init__(self):
        """Initialize your database connection here."""
        # self.db = db
        # self.username = username
        # self.password = password
        # self.port = port
        self.cur = None
        self.conn = None

    def __str__(self):
        return 'Database connection object'

    def connect(self):
        self.conn = psycopg2.connect("""
            host=rc1a-l1yd6f3tfekfeek0.mdb.yandexcloud.net
            port=6432
            dbname=db1
            user=user1
            password=4210610lk
            target_session_attrs=read-write
        """)
        self.cur = self.conn.cursor()

    def execute_query(self, query, vars):
        self.cur.execute(query, vars)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

def parse_coincap():
    r = requests.get('http://api.coincap.io/v2/rates/bitcoin')
    if r.status_code == 200:
        respsonse = r.json()
        asst_id = respsonse['data'].get('id')
        symbol = respsonse['data'].get('symbol')
        curr_symbol = respsonse['data'].get('currencySymbol')
        asst_type = respsonse['data'].get('type')
        rate_usd = respsonse['data'].get('rateUsd')
        ts = respsonse['timestamp']
        return (asst_id, symbol, curr_symbol, asst_type, rate_usd, ts)
    else:
        raise Exception("Check the request")

def write_to_db(asst_id, symbol, curr_symbol, asst_type, rate_usd, ts):
    c1 = DBConnection()
    c1.connect()
    dt_object = datetime.fromtimestamp(ts/1000)
    print(dt_object)
    vars = (asst_id, symbol, curr_symbol, asst_type, rate_usd, dt_object)
    
    if all(vars):
        table = "INSERT INTO coincap_rates.btc(asst_id, symbol, currencysymbol, type, rateusd, date_created) VALUES(%s,%s,%s,%s,%s,%s)"
        c1.execute_query(table, vars)
    else:
        raise Exception("Check the parser")

def main():
    asst_id, symbol, curr_symbol, asst_type, rate_usd, ts = parse_coincap()
    write_to_db(asst_id, symbol, curr_symbol, asst_type, rate_usd, ts)

if __name__ == "__main__":
    main()
