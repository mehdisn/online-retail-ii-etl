import clickhouse_connect

def test_tables_exist():
    client = clickhouse_connect.get_client(host="localhost", port=9000)
    names = {t[0] for t in client.query("SHOW TABLES FROM retail").result_rows}
    assert {"dim_customer","dim_product","dim_date","fact_sales"}.issubset(names)
