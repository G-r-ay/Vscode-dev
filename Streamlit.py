import requests
import pandas as pd
import datetime 
import http.client
from requests.exceptions import ConnectionError, Timeout, JSONDecodeError

#---------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------
api_key = 'ZSYW9UIY7ZT5XAWYIMRRA45GPD1SYG2A54'

url = "https://api.arbiscan.io/api"
#---------------------------------------------------------------------------------------------------------------

def first_last_info(address):

    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "page": 1,
        "offset": 100,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": api_key
    }

    response = requests.get(url, params=params).json()
    response = response['result']
    first_date = response[0]['timeStamp']
    last_date = response[-1]['timeStamp']

    first_to = response[0]['to']
    first_from = response[0]['from']

    last_to = response[-1]['to']
    last_from = response[-1]['from']


    first_out_amount = None
    last_out_amount = None

    for transaction in response:
        if transaction['from'].lower() == address.lower():
            value = int(transaction['value']) / 10**18  # Convert wei to ether
            if first_out_amount is None:
                first_out_amount = value
            last_out_amount = value

    if first_to == address:
        first_to = 'self'
    if first_from == address:
        first_from = 'self'
    if last_to == address:
        last_to = 'self'
    if last_from == address:
        last_from = 'self'

    out_count = 0  # Counter for transactions where the address is the sender
    in_count = 0   # Counter for transactions where the address is the receiver

    for transaction in response:
        if transaction['from'].lower() == address.lower():
            out_count += 1

        if transaction['to'].lower() == address.lower():
            in_count += 1
    if out_count == 0:
        return 0  # Avoid division by zero

    ratio  = round(in_count / out_count,3)

    first_in_amount = None
    last_in_amount = None

    for transaction in response:
        if transaction['to'].lower() == address.lower():
            value = int(transaction['value']) / 10**18  # Convert wei to ether
            if first_in_amount is None:
                first_in_amount = value
            last_in_amount = value

    return [first_date, last_date, first_from, first_to, last_from, last_to,ratio,first_out_amount,last_out_amount,first_in_amount, last_in_amount]
#---------------------------------------------------------------------------------------------------------------

def get_transaction_history(address):

        params = {
            "module": "account",
            "action": "txlist",
            "address": address,
            "page": 1,
            "offset": 100,
            "startblock": 0,
            "endblock": 99999999,
            "sort": "asc",
            "apikey": api_key
        }

        response = requests.get(url, params=params).json()
        return response['result']


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def get_Erc20_transaction_history(address):

    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "page": 1,
        "offset": 100,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": api_key
    }

    response = requests.get(url, params=params).json()
    return response['result']
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def get_suppler_contract(address):

    params = {
        "module": "account",
        "action": "txlistinternal",
        "address": address,
        "page": 1,
        "offset": 100,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": api_key
    }

    response = requests.get(url, params=params).json()
    return response['result'][-1]
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def get_wallet_age(history: list[dict]):
    if len(history) > 0:
        creation_time = int(history[0]['timeStamp'])
        creation_date = datetime.datetime.fromtimestamp(creation_time).date()
        current_date = datetime.date.today()
        wallet_age = (current_date - creation_date).days
        return wallet_age
    else:
        return 0
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def to_and_from(history: list[dict], address):
    from_count = 0
    to_count = 0
    for transactions in history:
        if transactions['from'] == address:
            from_count += 1
        else:
            to_count += 1
    return from_count, to_count
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def check_arb_balance(address):
    url = 'https://api.etherscan.io/api'

    # Parameters
    params = {
        'module': 'account',
        'action': 'tokenbalance',
        'contractaddress': '0xb50721bcf8d664c30412cfbc6cf7a15145234ad1',
        'address': address,
        'tag': 'latest',
        'apikey': api_key,
    }

    arb_balance = float(requests.get(url, params=params).json()['result'])
    if arb_balance != 0.0:
        return True
    else:
        return False
#-------------------------------------
def fetch(address, nested_list):

    reg_hist = get_transaction_history(address)
    trasacting_hist = first_last_info(address)
    erc20_hist = get_Erc20_transaction_history(address)

    txn_count = len(reg_hist)

    reg_age = get_wallet_age(reg_hist)
    erc_age = get_wallet_age(erc20_hist)

    reg_to, reg_from = to_and_from(reg_hist, address)
    erc_to, erc_from = to_and_from(erc20_hist, address)

    row = [address, txn_count, reg_age, erc_age, reg_to,reg_from, erc_to, erc_from] + trasacting_hist
    nested_list.append(row)

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def compile_data():
    data = pd.read_csv('ML_arbmatching.csv')
    addresses = data['voter'].unique()[:5]
    headers = ['voter','txn_count','Wallet_Age','Wallet_Age(Erc20)','to_count','from_count','erc_to','erc_from', 'first_date', 'last_date', 'first_from', 'first_to', 'last_from', 'last_to','in-out_ratio','first_out_amount','last_out_amount','first_in_amount','last_in_amount']
    contents = []
    for count, address in enumerate(addresses, start=0):
        try:
            fetch(address, contents)
            print(count)
        except (ConnectionError, Timeout, http.client.RemoteDisconnected, TypeError, JSONDecodeError,IndexError) as e:
            print(f'Failed to fetch data for {address}: Error Type {e}')
    new_data_df = pd.DataFrame(contents, columns=headers)
    new_data_df.to_csv('Arb_voters_data.csv',index=False)

compile_data()
