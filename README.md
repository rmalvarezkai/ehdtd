# Ehdtd - cryptoCurrency Exchange history data to database
This class retrieves historical data from exchanges and stores it in a database.

Example:
### Install
    ```bash
    pip install ehdtd
    ```
### Use

    ```python
    import time
    from ehdtd import Ehdtd

    exchange = 'binance'
    symbol = 'BTC/USDT'
    interval = '1m'
    limit = 10

    db_data = {
        'db_type': 'postgresql',  # postgresql, mysql
        'db_name': 'ehdtd',
        'db_user': 'ehdtd',
        'db_pass': 'xxxxxxxxx',
        'db_host': '127.0.0.1',
        'db_port': '5432'
    }

    fetch_data = [
        {
            'symbol': symbol,
            'interval': interval
        }
    ]

    ehd = Ehdtd(exchange, fetch_data, db_data)  # Create an instance
    ehd.start()  # Start fetching data

    time.sleep(900)  # First time Wait for available data, for the data to be updated,
                     # you must wait between 90 minutes and 2.5 hours depending on the interval

    for v in fetch_data:
        symbol = v['symbol']
        interval = v['interval']
        start_from = 0
        until_to = None
        return_type = 'pandas'
        data_db = ehd.get_data_from_db(symbol, interval, start_from, until_to, return_type)
        print(data_db)
        print('=========================================================================')
        print('')
        time.sleep(1)

    ehd.stop()  # Stop fetching data
    ```

# How It Works:

## For Binance:

1. Try to retrieve data from a file. Check this link:\
    [Binance Public Data](https://github.com/binance/binance-public-data/#trades-1)
2. If the file is not available, try to retrieve data from the API.
3. Then get data from the WebSocket API using the Ccxw class.

## Database Columns:

- `open_time`, `open_date`, `open_price`, `close_time`, `close_date`, `close_price`, `low`,
    `high`, `volume`, `exchange`, `symbol`, `interval`, `status`, `data`

- Column `data` is not used,\
    and column `status` can have three values: `'__NON_CHECK__'`, `'__OK__'`, `'__ERROR__'`.
    - If `status == '__OK__'`, the file has consistent data.
    - If `status == '__ERROR__'`, the file has inconsistent data.
    - If `status == '__NON_CHECK__'`, the file is not analyzed.

## Retrieving Data from Database:

Use the function `ehd.get_data_from_db(symbol, interval, start_from, until_to, return_type)`:
- If `return_type == 'pandas'`, it returns a Pandas DataFrame.
- If `return_type == 'list'`, it returns a list of dictionaries.

[View License](LICENSE)

