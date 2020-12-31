import requests  # for api calls
import bs4 as bs  # for creating list of sp500
import pickle  # for creating list of sp500
import pprint as pp  # prints json files in a more readable way, mostly for debugging
from pymongo import MongoClient
import time

def convert_epoch_to_datetime(epoch):
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
    data = requests.get('https://finnhub.io/api/v1/stock/profile2?symbol={}&token=br3gbbnrh5rai6tghkig'.format(ticker)).json()
    return data


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
        int[] epoch
    """
    data = requests.get('https://finnhub.io/api/v1/stock/candle?symbol={}&resolution={}&from={}&to={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate)).json()
    return data['o'], data['h'], data['l'], data['c'], data['v'], data['t']


def get_indicator_epoch(ticker, resolution, startDate, endDate):
    data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=sma&timeperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, 2)).json()
    return data['t']


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
    data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=ema&timeperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, timePeriod)).json()
    return data['ema']


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
    data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=rsi&timeperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, timePeriod)).json()
    return data['rsi']


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
    data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=stoch&fastkperiod={}&slowkperiod={}&slowdperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, fastKPeriod, slowKPeriod, slowDPeriod)).json()
    return data['slowd'], data['slowk']


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
    data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=macd&fastperiod={}&slowperiod={}&signalperiod={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, fastPeriod, slowPeriod, signalPeriod)).json()
    return data['macd'], data['macdSignal'], data['macdHist']


def get_obv(ticker, resolution, startDate, endDate):
    """
    Purpose: Get a stock's On-Balance Volume. TBH idk what this indicator does... so here's a website: https://www.investopedia.com/terms/o/onbalancevolume.asp
    Parameters:
        ticker: The Stock's ticker symbol
        resolution: The length of time in one period. Weekly chart vs daily chart vs minute chart etc.
            Supported Resolutions: '1', '5', '15', '30', '60', 'D', 'W', 'M'
        startDate: Gather stock price starting at this date. Use UNIX timestamp
            Examples: 1608429272
        endDate: Gather stock price ending at this date. Use same format as startDate parameter
    Return:
        double[] obv
    """
    data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=obv&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate)).json()
    return data['obv']


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
    data = requests.get('https://finnhub.io/api/v1/indicator?symbol={}&resolution={}&from={}&to={}&indicator=bbands&timeperiod={}&nbdevup={}&nbdevdn={}&token=br3gbbnrh5rai6tghkig'.format(ticker, resolution, startDate, endDate, timePeriod, nbdevUp, nbdevDown)).json()
    return data['lowerband'], data['middleband'], data['upperband']


def get_beta(ticker):
    """
    Purpose: Get a stock's beta. A financial statistic that tells you how volatile a stock is compared to the rest of the market. https://www.investopedia.com/terms/b/beta.asp
    Parameters:
        ticker: The Stock's ticker symbol
    Return:
        int beta  # returns a stock's 5 year beta
    """
    data = requests.get('https://finnhub.io/api/v1/stock/metric?symbol={}&metric=all&token=br3gbbnrh5rai6tghkig'.format(ticker)).json()
    return data['metric']['beta']



