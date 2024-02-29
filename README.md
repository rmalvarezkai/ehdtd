# Ehdtd - cryptoCurrency Exchange history data to database

This library is designed to fetch historic data from exchange an put data in database in background mode.

## Example:

###Install
```bash
pip install ehdtd
```
###Use

```python
import time
import pprint
from ehdtd import Ehdtd

exchange = 'binance'
symbol = 'BTC/USDT'
interval = '1H'

db_data = {}
db_data['db_name'] = 'ehdtd'
db_data['db_user'] = 'ehdtd'
db_data['db_host'] = '127.0.0.1'
db_data['db_type'] = 'postgresql'

fetch_data = []
fetch_data_n = {}
fetch_data_n['symbol'] = 'BTC/USDT'
fetch_data_n['interval'] = '1H'

fetch_data.append(fetch_data_n)

ehd = Ehdtd(exchange, fetch_data, db_data)  # Create instance

ehd.start()  # Start getting data

time.sleep(900)

ehd.stop()  # Stop getting data

```

### Important Information


[View License](LICENSE)
