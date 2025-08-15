# Binance Public Data
I want to write a script system that batch download binance public data it do:
1. read the user config at the vatiable or yaml file (file first, overwrite, could set time_range ...)
2. scan the website and download zip data file
3. unizp the file and delete orignal zip file
it should support Reconnect from breakpoint and threshold for binance limit and reconnect when facing temple internet issue (retry after serveral seconds)

now I give you detail webiste url rules and file header data type etc..

the base url is https://data.binance.vision/data/futures/um/daily/ as I only need futuers data at daily
its has several sub link 
* aggTrades/		
  inside is like https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-07-31.zip 
  header is like 
  agg_trade_id	price	quantity	first_trade_id	last_trade_id	transact_time	is_buyer_maker
  2804871623	117771.1	0.16	6514979178	6514979178	1.75392E+12	FALSE

* bookDepth/
  inside is like https://data.binance.vision/data/futures/um/daily/bookDepth/1000BONKUSDT/1000BONKUSDT-bookDepth-2025-07-31.zip
  header is like
  timestamp,percentage,depth,notional
  2025-07-31 00:00:07,-5,139762634.00000000,3886539.66954300
  2025-07-31 00:00:07,-4,116926083.00000000,3268150.53962000

* bookTicker/		
  inside is like https://data.binance.vision/data/futures/um/daily/bookTicker/1000BONKUSDT/1000BONKUSDT-bookTicker-2024-03-30.zip
  header is like update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,transaction_time,event_time
4307082988118,0.02634800,315928.00000000,0.02634900,2385.00000000,1711756800080,1711756800087
4307083859526,0.02634800,315563.00000000,0.02634900,2385.00000000,1711756801522,1711756802066

* indexPriceKlines/		
  inside has sub folder 12h/ 15m/ 1d/ 1h/ 1m/ 2h/ 30m/ 3m/ 4h/ 5m/ 6h/ 8h (should be configureable which of those should be download like ["12h","1m"])
  inside each subfolder is like https://data.binance.vision/data/futures/um/daily/indexPriceKlines/1000000BOBUSDT/1h/1000000BOBUSDT-1h-2025-07-31.zip
  header is like 
  open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
1753920000000,0.05850465,0.05864322,0.05814527,0.05849142,0,1753923599999,0,3600,0,0,0
1753923600000,0.05849142,0.05852996,0.05754051,0.05804089,0,1753927199999,0,3600,0,0,0
  
* klines/ 
  is like above , subfolder time sliped 
  inside eahc subdolder  is like https://data.binance.vision/data/futures/um/daily/klines/1000BONKUSDC/15m/1000BONKUSDC-15m-2025-07-31.zip
  open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
1753920000000,0.05850465,0.05864322,0.05814527,0.05849142,0,1753923599999,0,3600,0,0,0
1753923600000,0.05849142,0.05852996,0.05754051,0.05804089,0,1753927199999,0,3600,0,0,0

* markPriceKlines/	
  indeside is like above , url like https://data.binance.vision/data/futures/um/daily/markPriceKlines/1000000BOBUSDT/15m/1000000BOBUSDT-15m-2025-07-31.zip
  header is like 
  open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
1753660800000,0.06232000,0.06273896,0.06126000,0.06198568,0,1753661699999,0,900,0,0,0
1753661700000,0.06198578,0.06232338,0.06130033,0.06144000,0,1753662599999,0,900,0,0,0

* metrics/
  inside no time slip, url like https://data.binance.vision/data/futures/um/daily/metrics/1000000BOBUSDT/1000000BOBUSDT-metrics-2025-08-01.zip
  header is like 
  create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-08-01 00:05:00,1000000BOBUSDT,28094099.0000000000000000,1608949.0497300000000000,1.40508806,2.00440300,1.67633929,1.51837000
2025-08-01 00:10:00,1000000BOBUSDT,28125008.0000000000000000,1609716.8328748800000000,1.40039063,1.99821400,1.68497758,2.22311200	
* premiumIndexKlines/	
  indeisdie no time slip, url is like https://data.binance.vision/data/futures/um/daily/premiumIndexKlines/1000000BOBUSDT/15m/1000000BOBUSDT-15m-2025-07-31.zip
  header is like open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
1753920000000,0.00449955,0.00449955,0,0.00076832,0,1753920899999,0,180,0,0,0
1753920900000,0.00050278,0.00066572,0,0,0,1753921799999,0,180,0,0,0
  
* trades/
  inside is like https://data.binance.vision/data/futures/um/daily/trades/1000000MOGUSDT/1000000MOGUSDT-trades-2025-07-31.zip
  header is 
  id,price,qty,quote_qty,time,is_buyer_maker
66119037,1.4327,11.6,16.61932,1753920002864,true
66119038,1.4326,6.2,8.88212,1753920002864,true
66119039,1.4326,74.8,107.15848,1753920002870,true
66119040,1.4326,11.6,16.61816,1753920003180,true

