import os
import functools
import operator
import itertools
from time import sleep
import signal
import requests
import pandas as pd
import pandas_ta as ta
import re
import json

# this class definition allows printing error messages and stopping the program
class ApiException(Exception):
    pass

# this signal handler allows for a graceful shutdown when CTRL+C is pressed
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

# set your API key to authenticate to the RIT client
API_KEY = {'X-API-Key': 'type API-Key here'}
shutdown = False

# this helper method returns the current 'tick' of the running case
def get_tick(session):
    resp = session.get('http://localhost:9999/v1/case')
    if resp.status_code == 401:
        raise ApiException('Error getting tick: The API key provided in this Python code must match that in the RIT client')
    case = resp.json()
    return case['tick']

# this helper method returns the bid and ask for a given security
def ticker_bid_ask(session, ticker):
    payload = {'ticker': ticker}
    resp = session.get('http://localhost:9999/v1/securities/book', params=payload)
    if resp.ok:
        book = resp.json()
        return book['bids'][0]['price'], book['asks'][0]['price']
    raise ApiException('Error getting bid / ask: The API key provided in this Python code must match that in the RIT client')

# this is the main method containing the actual order routing logic
def main():
    
    with requests.Session() as s:
        # add the API key to the session to authenticate during requests
        s.headers.update(API_KEY)
        # get the current time of the case
        tick = get_tick(s)
        
        while 3 <= tick <= 300:
            tick = get_tick(s)

            # request 1highest bid and lowest ask
            bid_ask_A = s.get('http://localhost:9999/v1/securities/book', params={'ticker': 'CRZY_A', 'limit': 1})
            bid_ask_M = s.get('http://localhost:9999/v1/securities/book', params={'ticker': 'CRZY_M', 'limit': 1})

            # parse json object from API request object above to get bid and ask prices
            CRZY_A_bid_price = bid_ask_A.json()['bids'][0]['price']
            CRZY_A_ask_price = bid_ask_A.json()['asks'][0]['price']
            CRZY_M_bid_price = bid_ask_M.json()['bids'][0]['price']
            CRZY_M_ask_price = bid_ask_M.json()['asks'][0]['price']
            
            # # parse json object from API request object to get order quantity from bid and ask
            CRZY_A_bid_quantity_total = bid_ask_A.json()['bids'][0]['quantity']
            CRZY_A_ask_quantity_total = bid_ask_A.json()['asks'][0]['quantity']
            CRZY_M_bid_quantity_total = bid_ask_M.json()['bids'][0]['quantity']
            CRZY_M_ask_quantity_total = bid_ask_M.json()['asks'][0]['quantity']

            # parse order quantity filled
            CRZY_A_bid_quantity_filled = bid_ask_A.json()['bids'][0]['quantity_filled']
            CRZY_A_ask_quantity_filled = bid_ask_A.json()['asks'][0]['quantity_filled']
            CRZY_M_bid_quantity_filled = bid_ask_M.json()['bids'][0]['quantity_filled']
            CRZY_M_ask_quantity_filled = bid_ask_M.json()['asks'][0]['quantity_filled']

            # calcuate amount left to fill on the limit orders
            CRZY_A_bid_quantity = CRZY_A_bid_quantity_total - CRZY_A_bid_quantity_filled
            CRZY_A_ask_quantity = CRZY_A_ask_quantity_total - CRZY_A_ask_quantity_filled
            CRZY_M_bid_quantity = CRZY_M_bid_quantity_total - CRZY_M_bid_quantity_filled
            CRZY_M_ask_quantity = CRZY_M_ask_quantity_total - CRZY_M_ask_quantity_filled

            # quantity decision rule: sets order quantity to the minimum quantity between arbitrage trades
            if CRZY_A_bid_quantity > CRZY_M_ask_quantity:
                quantity1 = CRZY_M_ask_quantity
            else:
                quantity1 = CRZY_A_bid_quantity

            if CRZY_M_bid_quantity > CRZY_A_ask_quantity:
                quantity2 = CRZY_A_ask_quantity
            else:
                quantity2 = CRZY_M_bid_quantity

            # minimum price difference to conduct arbitrage trade
            price_cushion = .02

            # percentage of minimum quantity we want to trade (other algorithms may beat us to the order)
            quantity_ratio = .5

            # algorithm decision rule: arbitrage between markets
            if CRZY_A_bid_price > CRZY_M_ask_price + price_cushion:
                # send arbitraging market orders

                s.post('http://localhost:9999/v1/orders', params={'ticker': 'CRZY_M', 'type': 'MARKET', 'quantity': quantity1 * quantity_ratio, 'action': 'BUY'})
                s.post('http://localhost:9999/v1/orders', params={'ticker': 'CRZY_A', 'type': 'MARKET', 'quantity': quantity1 * quantity_ratio, 'action': 'SELL'})

                # do nothing for 0.25 seconds
                sleep(0.25)

            if CRZY_M_bid_price > CRZY_A_ask_price + price_cushion:
                # send arbitraging market orders

                s.post('http://localhost:9999/v1/orders', params={'ticker': 'CRZY_A', 'type': 'MARKET', 'quantity': quantity2 * quantity_ratio, 'action': 'BUY'})
                s.post('http://localhost:9999/v1/orders', params={'ticker': 'CRZY_M', 'type': 'MARKET', 'quantity': quantity2 * quantity_ratio, 'action': 'SELL'})
                
                # do nothing for 0.25 seconds
                sleep(0.25)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()