import requests  # for api calls
import time  # for converting epoch to timedate format
import pprint as pp  # for debugging


def convert_epoch_to_datetime(epoch):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch))


def get_news(ticker, startDate, endDate):
    """
    Purpose: Get a list of news urls pertaining to a stock
    :param ticker: Ticker symbol of a stock
    :param startDate: When to start looking for news articles. Format is: 'YYYY-MM-DD'
    :param endDate: When to stop looking for news articles. Format is: 'YYYY-MM-DD'
    :return:
        string[] headline
        string[] url
        string[] datetime  # format: 'YYYY-MM-DD HH:MM:SS'
    """
    datetime = []
    headline = []
    url = []
    data = requests.get('https://finnhub.io/api/v1/company-news?symbol={}&from={}&to={}&token=br3gbbnrh5rai6tghkig'.format(ticker, startDate, endDate)).json()
    for article in range(len(data)):
        headline.append(data[article]['headline'])
        url.append(data[article]['url'])
        datetime.append(convert_epoch_to_datetime(data[article]['datetime']))
    return headline, url, datetime
