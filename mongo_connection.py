import time
from pymongo import MongoClient
from data_application.api_calls import technical_indicator_retriever as tc

# establish mongodb connection
client = MongoClient('localhost', 27017)
db = client['Stock_Intellegence']
db.drop_collection()
db.create_collection('Stock_Intell')


def build_ticker_data(ticker, ticker_data, counter):
    # get company profile
    ticker_data['ticker'] = ticker
    company_profile = tc.get_company_profile(ticker)
    ticker_data['name'] = company_profile['name']
    ticker_data['industry'] = company_profile['finnhubIndustry']
    ticker_data['exchange'] = company_profile['exchange']
    ticker_data['ipo'] = company_profile['ipo']
    ticker_data['currency'] = company_profile['currency']

    # get prices
    ticker_data['price'] = []
    resolution_ls = ['D', 'W', 'M']
    for resolution in resolution_ls:
        open_ls, high_ls, low_ls, close_ls, volume_ls, epoch_ls = tc.get_price_info(ticker, resolution, y2020, int(
            time.time()))  # 1262304000 is epoch for the beginning of year 2010
        if resolution == 'D':
            resolution_label = 'daily_prices'
        elif resolution == 'W':
            resolution_label = 'weekly_prices'
        else:
            resolution_label = 'monthly_prices'
        price_dict = {resolution_label: []}
        for i in range(len(epoch_ls)):
            date = tc.convert_epoch_to_datetime(epoch_ls[i])
            open = open_ls[i]
            high = high_ls[i]
            low = low_ls[i]
            close = close_ls[i]
            volume = volume_ls[i]
            date_dict = {
                date: {
                    'open': open,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume
                }
            }
            price_dict[resolution_label].append(date_dict)
        ticker_data['price'].append(price_dict)

    # get indicators
    indicators_dict = {}

    # get ema's
    ema_dict = {
        'daily_ema': [],
        'weekly_ema': [],
        'monthly_ema': []
    }
    ticker_data['indicators'] = []
    # get daily ema
    daily_ema_timeperiods_ls = [2, 5, 10, 21, 50, 100, 150, 200]
    daily_epoch_ls = tc.get_indicator_epoch(ticker, 'D', y2020, int(time.time()))
    daily_ema_2d_ls = []
    for timeperiod in daily_ema_timeperiods_ls:
        daily_ema_2d_ls.append(tc.get_ema(ticker, 'D', y2020, int(time.time()), timeperiod))
        # time.sleep(1)  # delay to avoid the 60 api calls / min limitation that finnhub imposes
    for i in range(len(daily_epoch_ls)):
        date = tc.convert_epoch_to_datetime(daily_epoch_ls[i])
        date_dict = {
            date: {
                '2-day': daily_ema_2d_ls[0][i],
                '5-day': daily_ema_2d_ls[1][i],
                '10-day': daily_ema_2d_ls[2][i],
                '21-day': daily_ema_2d_ls[3][i],
                '50-day': daily_ema_2d_ls[4][i],
                '100-day': daily_ema_2d_ls[5][i],
                '200-day': daily_ema_2d_ls[6][i]
            }
        }
        ema_dict['daily_ema'].append(date_dict)
    # get weekly ema
    weekly_ema_timeperiods_ls = [1, 2, 4, 10, 20, 30, 40]
    weekly_epoch_ls = tc.get_indicator_epoch(ticker, 'W', y2020, int(time.time()))
    weekly_ema_2d_ls = []
    for timeperiod in weekly_ema_timeperiods_ls:
        weekly_ema_2d_ls.append(tc.get_ema(ticker, 'W', y2020, int(time.time()), timeperiod))
        # time.sleep(1)
    for i in range(len(weekly_epoch_ls)):
        date = tc.convert_epoch_to_datetime(weekly_epoch_ls[i])
        date_dict = {
            date: {
                '1-week': weekly_ema_2d_ls[0][i],
                '2-week': weekly_ema_2d_ls[1][i],
                '4-wwek': weekly_ema_2d_ls[2][i],
                '10-week': weekly_ema_2d_ls[3][i],
                '20-week': weekly_ema_2d_ls[4][i],
                '30-week': weekly_ema_2d_ls[5][i],
                '40-week': weekly_ema_2d_ls[6][i]
            }
        }
        ema_dict['weekly_ema'].append(date_dict)
    # get monthly ema
    monthly_ema_timeperiods_ls = [1, 2, 5, 7, 8, 10]
    monthly_epoch_ls = tc.get_indicator_epoch(ticker, 'M', y2020, int(time.time()))
    monthly_ema_2d_ls = []
    for timeperiod in monthly_ema_timeperiods_ls:
        monthly_ema_2d_ls.append(tc.get_ema(ticker, 'M', y2020, int(time.time()), timeperiod))
        # time.sleep(1)
    for i in range(len(monthly_epoch_ls)):
        date = tc.convert_epoch_to_datetime(monthly_epoch_ls[i])
        date_dict = {
            date: {
                '1-month': monthly_ema_2d_ls[0][i],
                '2-month': monthly_ema_2d_ls[1][i],
                '5-month': monthly_ema_2d_ls[2][i],
                '7-month': monthly_ema_2d_ls[3][i],
                '8-month': monthly_ema_2d_ls[4][i],
                '10-month': monthly_ema_2d_ls[5][i]
            }
        }
        ema_dict['monthly_ema'].append(date_dict)

    ticker_data['indicators'] = ema_dict
    counter += 1
    return ticker_data


def store_ticker_data_to_db():
    sp500 = tc.create_sp500_list()     # create a list of sp500 tickers
    # print(sp500)
    collection = db['SP500']
    for ticker in sp500:
        ticker_data = build_ticker_data(ticker, {}, 0)
        collection.insert_one(ticker_data)


y2010 = 1262304000
y2020 = 1577836800

store_ticker_data_to_db()