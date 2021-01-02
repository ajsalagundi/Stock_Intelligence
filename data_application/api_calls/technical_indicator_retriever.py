import requests  # for api calls
import bs4 as bs  # for creating list of sp500
import pickle  # for creating list of sp500
import pprint as pp  # prints json files in a more readable way, mostly for debugging
from pymongo import MongoClient
import time

def convert_epoch_to_datetime(epoch):
    """
    *****************************IMPORTANT******************************
    * THIS MAY NEED TO CHANGE WHEN GIVEN CUSTOMERS IN OTHER TIME ZONES *                                ***IMPORTANT***
    * Epoch uses UTC, while I am in CST                                *
    *****************************IMPORTANT******************************
    """
    epoch = epoch + 21600  # add 6 hours to epoch in order to account for time zone, PLEASE CHANGE THIS WHEN ADJUSTING FOR TIME ZONE
    return time.strftime('%Y-%m-%d', time.localtime(epoch))


def create_sp500_list():
    """
    Purpose: Scrape wikipedia s&p500 article to create a list of ticker symbols in the s&p500
    Return:
        string[] tickers
    """
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)
    with open("sp500tickers.pickle", "wb") as f:
        pickle.dump(tickers, f)
    tickers = list(map(lambda s: s.strip(), tickers))  # remove \n after each ticker
    return tickers


def get_company_profile(ticker):
    """
    Purpose: Get basic company info like: country, currency, exchange, ipo, marketCap, name, phone, shareOutstanding, ticker, weburl, logo, industry
    :param ticker: The ticker of a stock
    :return:
        Example Return:
            {
              "country": "US",
              "currency": "USD",
              "exchange": "NASDAQ/NMS (GLOBAL MARKET)",
              "ipo": "1980-12-12",
              "marketCapitalization": 1415993,
              "name": "Apple Inc",
              "phone": "14089961010",
              "shareOutstanding": 4375.47998046875,
              "ticker": "AAPL",
              "weburl": "https://www.apple.com/",
              "logo": "https://static.finnhub.io/logo/87cb30d8-80df-11ea-8951-00000000092a.png",
              "finnhubIndustry":"Technology"
            }
    """
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/stock/profile2?symbol={}&token=br3gbbnrh5rai6tghkig'.format(ticker)).json()
            return data
        except:
            print('api call error occured... waiting ', delay, ' seconds, then recalling api')
            time.sleep(delay)


def get_price_info(ticker, resolution, startDate, endDate):
    """
    Purpose: Get a stock's open price, close price, high price, low price, and volume within a certain time period
    Parameters:
        ticker: The stock's ticker symbol
        resolution: The length of time in one period. Weekly chart vs daily chart vs minute chart etc.
            Supported Resolutions: '1', '5', '15', '30', '60', 'D', 'W', 'M'
        startDate: Gather stock price starting at this date. Use UNIX timestamp
            Examples: 1608429272
        endDate: Gather stock price ending at this date. Use same format as startDate parameter
    Returns:
        double[] openPrice
        double[] highPrice
        double[] lowPrice
        double[] closePrice
        int[] volume
    """
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/stock/candle?symbol={}&resolution={}&from={}&to={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate)).json()
            return data['o'], data['h'], data['l'], data['c'], data['v']
        except:
            print('api call error occured... waiting ', delay, ' seconds, then recalling api')
            time.sleep(delay)


def get_indicator_epoch(ticker, resolution, startDate, endDate):
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=sma&timeperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, 2)).json()
            return data['t']
        except:
            print('api call error occured... recalling api')
            time.sleep(delay)


def get_ema(ticker, resolution, startDate, endDate, timePeriod):
    """
    Purpose: Get a stock's exponential moving average. https://www.investopedia.com/terms/e/ema.asp
    Parameters:
        ticker: The Stock's ticker symbol
        resolution: The length of time in one period. Weekly chart vs daily chart vs minute chart etc.
            Supported Resolutions: '1', '5', '15', '30', '60', 'D', 'W', 'M'
        startDate: Gather stock price starting at this date. Use UNIX timestamp
            Examples: 1608429272
        endDate: Gather stock price ending at this date. Use same format as startDate parameter
        timePeriod: how many time periods the ema should consider. A 3 day ema would consider 3 days; A 5 week ema would consider 5 weeks etc.
    Return:
        double[] ema
    """
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=ema&timeperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, timePeriod)).json()
            return data['ema']
        except:
            print('api call error occured... waiting ', delay, ' seconds, then recalling api')
            time.sleep(delay)


def get_rsi(ticker, resolution, startDate, endDate, timePeriod):
    """
    Purpose: Get a stock's Relative Strength Index. How well or poorly a stock does against the market. https://www.investopedia.com/terms/r/rsi.asp
    Parameters:
        ticker: The Stock's ticker symbol
        resolution: The length of time in one period. Weekly chart vs daily chart vs minute chart etc.
            Supported Resolutions: '1', '5', '15', '30', '60', 'D', 'W', 'M'
        startDate: Gather stock price starting at this date. Use UNIX timestamp
            Examples: 1608429272
        endDate: Gather stock price ending at this date. Use same format as startDate parameter
        timePeriod: how many time periods the rsi should consider
    Return:
        double[] rsi
    """
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=rsi&timeperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, timePeriod)).json()
            return data['rsi']
        except:
            print('api call error occured... waiting ', delay, ' seconds, then recalling api')
            time.sleep(delay)


def get_stoch(ticker, resolution, startDate, endDate, fastKPeriod, slowKPeriod, slowDPeriod):
    """
    Purpose: Get a stock's Stochastic Oscillator indicators. Momentum indicator comparing a particular closing price of a security to a range of its prices. Gets slow d and slow k. https://www.investopedia.com/terms/s/stochasticoscillator.asp
    Parameters:
        ticker: The Stock's ticker symbol
        resolution: The length of time in one period. Weekly chart vs daily chart vs minute chart etc.
            Supported Resolutions: '1', '5', '15', '30', '60', 'D', 'W', 'M'
        startDate: Gather stock price starting at this date. Use UNIX timestamp
            Examples: 1608429272
        endDate: Gather stock price ending at this date. Use same format as startDate parameter
        fastKPeriod: %K
        slowKPeriod: smoothing period   #I honestly am unsure about slowKPeriod and slowDPeriod
        slowDPeriod: %D
    Return:
        double[] slowd
        double[] slowk
    """
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=stoch&fastkperiod={}&slowkperiod={}&slowdperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, fastKPeriod, slowKPeriod, slowDPeriod)).json()
            return data['slowk'], data['slowd']
        except:
            print('api call error occured... waiting ', delay, ' seconds, then recalling api')
            time.sleep(delay)



def get_macd(ticker, resolution, startDate, endDate, fastPeriod, slowPeriod, signalPeriod):
    """
    Purpose: Get a stock's macd lines. Momentum indicator that is used as a trigger indicator. https://www.investopedia.com/terms/m/macd.asp#:~:text=Moving%20average%20convergence%20divergence%20(MACD)%20is%20a%20trend%2Dfollowing,from%20the%2012%2Dperiod%20EMA.
    Parameters:
        ticker: The Stock's ticker symbol
        resolution: The length of time in one period. Weekly chart vs daily chart vs minute chart etc.
            Supported Resolutions: '1', '5', '15', '30', '60', 'D', 'W', 'M'
        startDate: Gather stock price starting at this date. Use UNIX timestamp
            Examples: 1608429272
        endDate: Gather stock price ending at this date. Use same format as startDate parameter
        fastPeriod: the shorter period ema
        slowPeriod: the longer period ema. The MACD line is calculated through: (slowPeriod ema) - (fastPeriod ema)
        signalPeriod: ema of the MACD line. This is called the macd signal line
    Return:
        double[] macd  # the MACD line
        double[] macdSignal  # MACD signal line
        double[] macdHist  # the MACD Histogram line, calculated through: (MACD line) - (MACD signal line)
    """
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=macd&fastperiod={}&slowperiod={}&signalperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, fastPeriod, slowPeriod, signalPeriod)).json()
            return data['macd'], data['macdSignal'], data['macdHist']
        except:
            print('api call error occured... waiting ', delay, ' seconds, then recalling api')
            time.sleep(delay)


def get_bbands(ticker, resolution, startDate, endDate, timePeriod, nbdevUp, nbdevDown):
    """
    Purpose: Get a stock's bollingerband lines. An indicator to indicate over bought or over sold. https://www.investopedia.com/terms/b/bollingerbands.asp
    Parameters:
        ticker: The Stock's ticker symbol
        resolution: The length of time in one period. Weekly chart vs daily chart vs minute chart etc.
            Supported Resolutions: '1', '5', '15', '30', '60', 'D', 'W', 'M'
        startDate: Gather stock price starting at this date. Use UNIX timestamp
            Examples: 1608429272
        endDate: Gather stock price ending at this date. Use same format as startDate parameter
        timePeriod: the amount of time in a period for the simple moving average used in calculating bbands.
        nbdevUp: the upper band line
        nbdevDown: the lower band line
    Return:
        double[] lowerband
        double[] middleband  # the simple moving average used to calculate lower and upper bands
        double[] upperband
    """
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=bbands&timeperiod={}&nbdevup={}&nbdevdn={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, timePeriod, nbdevUp, nbdevDown)).json()
            return data['lowerband'], data['middleband'], data['upperband']
        except:
            print('api call error occured... waiting ', delay, ' seconds, then recalling api')
            time.sleep(delay)



def get_beta(ticker):
    """
    Purpose: Get a stock's beta. A financial statistic that tells you how volatile a stock is compared to the rest of the market. https://www.investopedia.com/terms/b/beta.asp
    Parameters:
        ticker: The Stock's ticker symbol
    Return:
        int beta  # returns a stock's 5 year beta
    """
    while True:
        try:
            data = requests.get('https://finnhub.io/api/v1/stock/metric?symbol={}&metric=all&token=br3gbbnrh5rai6tghkig'.format(ticker)).json()
            return data['metric']['beta']
        except:
            print('api call error occured... waiting ', delay, ' seconds, then recalling api')
            time.sleep(delay)



def store_ticker_data_to_db():
    sp500 = create_sp500_list()     # create a list of s&p500 tickers
    # establish mongodb connection and insert
    client = MongoClient('localhost', 27017)
    db = client['Stock_Intellegence']

    ticker_data = {}    # dictionary used to create json file for mongodb
    for ticker in sp500:
        # get company profile
        ticker_data['ticker'] = ticker
        company_profile = get_company_profile(ticker)
        ticker_data['name'] = company_profile['name']
        ticker_data['industry'] = company_profile['finnhubIndustry']
        ticker_data['exchange'] = company_profile['exchange']
        ticker_data['ipo'] = company_profile['ipo']
        ticker_data['currency'] = company_profile['currency']
        ticker_data['beta'] = get_beta(ticker)

        # get dates
        daily_epoch_ls = get_indicator_epoch(ticker, 'D', y2010, int(time.time()))
        weekly_epoch_ls = get_indicator_epoch(ticker, 'W', y2010, int(time.time()))
        weekly_epoch_ls = [x+86400 for x in weekly_epoch_ls]  # add 1 day to each element in weekly_epoch_ls to make it consistent with thinkorswim
        monthly_epoch_ls = get_indicator_epoch(ticker, 'M', y2010, int(time.time()))
        monthly_epoch_ls = [x-172800 for x in monthly_epoch_ls]  # subtract 2 days from each element in monthly_epoch_ls to make it consistent with thinkorswim

        # get prices
        ticker_data['price'] = []
        resolution_ls = ['D', 'W', 'M']
        for resolution in resolution_ls:
            open_ls, high_ls, low_ls, close_ls, volume_ls = get_price_info(ticker, resolution, y2010, int(time.time()))  # 1262304000 is epoch for the beginning of year 2010
            if resolution == 'D':
                resolution_label = 'daily_prices'
                epoch_ls = daily_epoch_ls
            elif resolution == 'W':
                resolution_label = 'weekly_prices'
                epoch_ls = weekly_epoch_ls
            else:
                resolution_label = 'monthly_prices'
                epoch_ls = monthly_epoch_ls
            price_dict = {resolution_label: []}
            for i in range(len(epoch_ls)):
                date = convert_epoch_to_datetime(epoch_ls[i])
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
        indicators_dict = {
            'ema': [],
            'rsi': [],
            'macd': [],
            'stoch': [],
            'bband': [],
        }

        # get ema's
        ema_dict = {
            'daily': [],
            'weekly': [],
            'monthly': []
        }
        ticker_data['indicators'] = []
        # get daily ema
        daily_ema_timeperiods_ls = [2, 5, 10, 20, 50, 100, 150, 200]
        daily_ema_2d_ls = []
        for timeperiod in daily_ema_timeperiods_ls:
            daily_ema_2d_ls.append(get_ema(ticker, 'D', y2010, int(time.time()), timeperiod))
            #time.sleep(1)  # delay to avoid the 60 api calls / min limitation that finnhub imposes
        for i in range(len(daily_epoch_ls)):
            date = convert_epoch_to_datetime(daily_epoch_ls[i])
            date_dict = {
                date: {
                    '2-day': daily_ema_2d_ls[0][i],
                    '5-day': daily_ema_2d_ls[1][i],
                    '10-day': daily_ema_2d_ls[2][i],
                    '20-day': daily_ema_2d_ls[3][i],
                    '50-day': daily_ema_2d_ls[4][i],
                    '100-day': daily_ema_2d_ls[5][i],
                    '200-day': daily_ema_2d_ls[6][i]
                }
            }
            ema_dict['daily'].append(date_dict)
        # get weekly ema
        weekly_ema_timeperiods_ls = [2, 4, 10, 20, 30, 40]
        weekly_ema_2d_ls = []
        for timeperiod in weekly_ema_timeperiods_ls:
            weekly_ema_2d_ls.append(get_ema(ticker, 'W', y2010, int(time.time()), timeperiod))
        for i in range(len(weekly_epoch_ls)):
            date = convert_epoch_to_datetime(weekly_epoch_ls[i])
            date_dict = {
                date: {
                    '2-week': weekly_ema_2d_ls[0][i],
                    '4-wwek': weekly_ema_2d_ls[1][i],
                    '10-week': weekly_ema_2d_ls[2][i],
                    '20-week': weekly_ema_2d_ls[3][i],
                    '30-week': weekly_ema_2d_ls[4][i],
                    '40-week': weekly_ema_2d_ls[5][i]
                }
            }
            ema_dict['weekly'].append(date_dict)
        # get monthly ema
        monthly_ema_timeperiods_ls = [2, 5, 7, 8, 10]
        monthly_epoch_ls = get_indicator_epoch(ticker, 'M', y2010, int(time.time()))
        monthly_ema_2d_ls = []
        for timeperiod in monthly_ema_timeperiods_ls:
            monthly_ema_2d_ls.append(get_ema(ticker, 'M', y2010, int(time.time()), timeperiod))
        for i in range(len(monthly_epoch_ls)):
            date = convert_epoch_to_datetime(monthly_epoch_ls[i])
            date_dict = {
                date: {
                    '2-month': monthly_ema_2d_ls[0][i],
                    '5-month': monthly_ema_2d_ls[1][i],
                    '7-month': monthly_ema_2d_ls[2][i],
                    '8-month': monthly_ema_2d_ls[3][i],
                    '10-month': monthly_ema_2d_ls[4][i]
                }
            }
            ema_dict['monthly'].append(date_dict)
        indicators_dict['ema'] = ema_dict

        # get rsi
        rsi_dict = {
            'daily': [],
            'weekly': []
        }
        daily_rsi_timeperiods_ls = [5, 7, 9, 14, 21, 28]
        daily_rsi_2d_ls = []
        for timeperiod in daily_rsi_timeperiods_ls:
            daily_rsi_2d_ls.append(get_rsi(ticker, 'D', y2010, int(time.time()), timeperiod))
        for i in range(len(daily_epoch_ls)):
            date = convert_epoch_to_datetime(daily_epoch_ls[i])
            date_dict = {
                date: {
                    '5-day': daily_rsi_2d_ls[0][i],
                    '7-day': daily_rsi_2d_ls[1][i],
                    '9-day': daily_rsi_2d_ls[2][i],
                    '14-day': daily_rsi_2d_ls[3][i],
                    '21-day': daily_rsi_2d_ls[4][i],
                    '28-day': daily_rsi_2d_ls[5][i]
                }
            }
            rsi_dict['daily'].append(date_dict)
        weekly_rsi_timeperiods_ls = [2, 3, 4]
        weekly_rsi_2d_ls = []
        for timeperiod in weekly_rsi_timeperiods_ls:
            weekly_rsi_2d_ls.append(get_rsi(ticker, 'W', y2010, int(time.time()), timeperiod))
        for i in range(len(weekly_epoch_ls)):
            date = convert_epoch_to_datetime(weekly_epoch_ls[i])
            date_dict = {
                date: {
                    '2-week': weekly_rsi_2d_ls[0][i],
                    '3-week': weekly_rsi_2d_ls[1][i],
                    '4-week': weekly_rsi_2d_ls[2][i]
                }
            }
            rsi_dict['weekly'].append(date_dict)
        indicators_dict['rsi'] = rsi_dict

        # get macd
        macd_dict = {
            'daily': [],
            'weekly': []
        }
        daily_macd_line, daily_signal_line, daily_macd_histogram = get_macd(ticker, 'D', y2010, int(time.time()), 12, 26, 9)
        for i in range(len(daily_epoch_ls)):
            date = convert_epoch_to_datetime(daily_epoch_ls[i])
            date_dict = {
                date: {
                    'macd': daily_macd_line[i],
                    'macd_signal': daily_signal_line[i],
                    'macd_histogram': daily_macd_histogram[i]
                }
            }
            macd_dict['daily'].append(date_dict)
        weekly_macd_line, weekly_signal_line, weekly_macd_histogram = get_macd(ticker, 'W', y2010, int(time.time()), 12, 26, 9)
        for i in range(len(weekly_epoch_ls)):
            date = convert_epoch_to_datetime(weekly_epoch_ls[i])
            date_dict = {
                date: {
                    'macd': weekly_macd_line[i],
                    'macd_signal': weekly_signal_line[i],
                    'macd_histogram': weekly_macd_histogram[i]
                }
            }
            macd_dict['weekly'].append(date_dict)
        indicators_dict['macd'] = macd_dict

        # get stoch
        stoch_dict = {
            'daily': [],
            'weekly': []
        }
        daily_slowk, daily_slowd = get_stoch(ticker, 'D', y2010, int(time.time()), 14, 3, 3)
        for i in range(len(daily_epoch_ls)):
            date = convert_epoch_to_datetime(daily_epoch_ls[i])
            date_dict = {
                date: {
                    'slowd': daily_slowd[i],
                    'slowk': daily_slowk[i]
                }
            }
            stoch_dict['daily'].append(date_dict)
        weekly_slowk, weekly_slowd = get_stoch(ticker, 'W', y2010, int(time.time()), 14, 3, 3)
        for i in range(len(weekly_epoch_ls)):
            date = convert_epoch_to_datetime(weekly_epoch_ls[i])
            date_dict = {
                date: {
                    'slowd': weekly_slowd[i],
                    'slowk': weekly_slowk[i]
                }
            }
            stoch_dict['weekly'].append(date_dict)
        indicators_dict['stoch'] = stoch_dict

        # get bbands
        bband_dict = {
            'daily': [],
            'weekly': []
        }
        daily_lowerband, daily_middleband, daily_upperband = get_bbands(ticker, 'D', y2010, int(time.time()), 20, 2, 2)
        for i in range(len(daily_epoch_ls)):
            date = convert_epoch_to_datetime(daily_epoch_ls[i])
            date_dict = {
                date: {
                    'lowerband': daily_lowerband[i],
                    'middleband': daily_middleband[i],
                    'upperband': daily_upperband[i]
                }
            }
            bband_dict['daily'].append(date_dict)
        weekly_lowerband, weekly_middleband, weekly_upperband = get_bbands(ticker, 'W', y2010, int(time.time()), 20, 2, 2)
        for i in range(len(weekly_epoch_ls)):
            date = convert_epoch_to_datetime(weekly_epoch_ls[i])
            date_dict = {
                date: {
                    'lowerband': weekly_lowerband[i],
                    'middleband': weekly_middleband[i],
                    'upperband': weekly_upperband[i]
                }
            }
            bband_dict['weekly'].append(date_dict)
        indicators_dict['bband'] = bband_dict

        ticker_data['indicators'] = indicators_dict

        # insert ticker_data dictionary
        collection = db[ticker]
        collection.insert_one(ticker_data)


y2010 = 1262304000  # the year 2010 in epoch
delay = 10  # if there is an error with API call, wait this delay in seconds, then recall api

store_ticker_data_to_db()


