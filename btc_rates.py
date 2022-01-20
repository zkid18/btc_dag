import requests
import psycopg2

from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timezone


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
        raise Exception("Failed to execute request", r.status_code)

def load_data(asst_id, symbol, curr_symbol, asst_type, rate_usd, ts):
    conn = PostgresHook(postgres_conn_id="crypto_postgres").get_conn()
    cursor = conn.cursor()

    dt_object = datetime.now(timezone.utc)
    vars = (asst_id, symbol, curr_symbol, asst_type, rate_usd, dt_object)
    print(vars)
    query = "INSERT INTO coincap_rates.btc(asst_id, symbol, currencysymbol, type, rateusd, date_created) VALUES(%s,%s,%s,%s,%s,%s) RETURNING asst_id"
    try:
        cursor.execute(query, vars)
    except Exception as err:
        conn.rollback()
        raise AirflowFailException("Failed executing the query {} with  error {}".format(query, err))
    cursor.close()
    conn.close()

def main():
    asst_id, symbol, curr_symbol, asst_type, rate_usd, ts = parse_coincap()
    load_data(asst_id, symbol, curr_symbol, asst_type, rate_usd, ts)

if __name__ == "__main__":
    main()
