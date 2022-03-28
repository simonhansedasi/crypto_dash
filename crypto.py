import pandas as pd
import random


def build_wallet():
    #start with exodus wallet
    pth = '/home/simon/stuff/coding/crypto_dash/data/'
    exo = pd.read_csv(pth + '20220311_exodus.csv',
                      na_filter=False,delimiter=',', quotechar='"')
    nexo_raw = pd.read_csv(pth + '20220311_nexo.csv')
    uphold_raw = pd.read_csv(pth + '20220311_uphold.csv')
    bfi = pd.read_csv(pth + '20220311_blockfi.csv')

    #to more easily track txid extract into different datasets and merge later. 
    in_idx = exo.index[exo['OUTTXID']=='']
    exo_in = exo.loc[in_idx]
    exo_in = exo_in[[
        'DATE',
        'INAMOUNT',
        'INCURRENCY',
        'TOADDRESS',
        'INTXID',
        'INTXURL',
        'TYPE'
    ]]
    exo_in.rename(columns = {
        'INAMOUNT':'AMOUNT',
        'INCURRENCY':'CURRENCY',
        'TOADDRESS':'ADDRESS',
        'INTXID':'TXID',
        'INTXURL':'TXURL'
    },inplace = True)

    out_idx = exo.index[exo['INTXID']=='']
    exo_out = exo.loc[out_idx]
    exo_out = exo_out[[
        'DATE',
        'OUTAMOUNT',
        'OUTCURRENCY',
        'TOADDRESS',
        'OUTTXID',
        'OUTTXURL',
        'TYPE'
    ]]
    exo_out.rename(columns = {
        'OUTAMOUNT':'AMOUNT',
        'OUTCURRENCY':'CURRENCY',
        'TOADDRESS':'ADDRESS',
        'OUTTXID':'TXID',
        'OUTTXURL':'TXURL'
    },inplace = True)

    fee_idx = exo.index[exo['FEEAMOUNT'] !='']
    exo_fee = exo.loc[fee_idx]
    exo_fee = exo_fee[[
        'DATE',
        'FEEAMOUNT',
        'FEECURRENCY',
        'TOADDRESS',
        'OUTTXID',
        'OUTTXURL',
        'TYPE'
    ]]
    exo_fee['TYPE']='fee'
    exo_fee.rename(columns = {
        'FEEAMOUNT':'AMOUNT',
        'FEECURRENCY':'CURRENCY',
        'TOADDRESS':'ADDRESS',
        'OUTTXID':'TXID',
        'OUTTXURL':'TXURL'
    },inplace = True)


    exodus = pd.concat([exo_in,exo_out,exo_fee],ignore_index=True)
    exodus['DATE'] = exodus['DATE'].str[4:]
    exodus['DATE'] = exodus['DATE'].str[:-33]   
    exodus['DATE'] = pd.to_datetime(exodus['DATE'],utc = True)

    nexo = nexo_raw[[
        'Currency',
        'Type',
        'Amount',
        'Date / Time'
    ]]

    nexo_raw = pd.read_csv('~/stuff/coding/crypto_dash/data/20220311_nexo.csv')
    nexo = nexo_raw
    nexo.rename(columns = {'Currency':'CURRENCY',
                           'Type':'TYPE',
                           'Amount':'AMOUNT',
                           'Date / Time':'DATE',
    #                        'USD Equivalent':'TOTAL'
                          },
               inplace = True)
    
    nexos = nexo.loc[nexo['CURRENCY'] == 'NEXONEXO']
    nexos['CURRENCY'] = 'NEXO'
    
    nexo = nexo.drop(nexos.index)
    
    idx = nexo.index[nexo['TYPE'] == 'Deposit']
    d = nexo.loc[idx]
    d['TYPE']='deposit'
    idx = nexo.index[nexo['TYPE'] == 'Withdrawal']
    w = nexo.loc[idx]
    w['TYPE']='withdrawal'
    idx = nexo.index[nexo['TYPE'] == 'Interest']
    i = nexo.loc[idx]
    i['TYPE']='interest'
    nexo = pd.concat([d,w,i,nexos])
    nexo = nexo.drop([
        'Transaction',
        'USD Equivalent',
        'Details',
        'Outstanding Loan'
    ],axis=1)
    nexo['DATE'] = pd.to_datetime(nexo['DATE'],utc = True)

    
    uphold = uphold_raw
    uphold = uphold.drop([
        'Id',
        'Status'
    ],axis=1)
    uphold.rename(columns = {
        'Date':'DATE'
    },inplace = True)

    # Uphold Deposits
    idx_dep = (uphold.index[uphold['Destination'] == 'uphold'] &
               uphold.index[uphold['Origin']=='uphold'] |
               uphold.index[uphold['Origin']=='ethereum']
              )

    uphold_dep = uphold.loc[idx_dep]
    uphold_dep.rename(columns = {'Destination Amount':'AMOUNT',
                                 'Destination Currency':'CURRENCY'}
                       ,inplace=True)
    uphold_dep = uphold_dep.drop([
        'Fee Amount',
        'Fee Currency',
        'Origin',
        'Origin Amount',
        'Origin Currency',
        'Destination',
        'Type'
    ],axis=1)
    uphold_dep['TYPE'] = 'deposit'

    # Uphold Fees
    idx_fee = uphold.index[uphold['Fee Currency'] == 'BAT']
    uphold_fee = uphold.loc[idx_fee]
    uphold_fee.rename(columns = {
        'Fee Amount':'AMOUNT',
        'Fee Currency':'CURRENCY'
    },inplace=True)

    uphold_fee = uphold_fee.drop([
        'Destination',
        'Destination Amount',
        'Destination Currency',
        'Origin',
        'Origin Currency',
        'Origin Amount',
        'Type'
    ],axis=1)
    uphold_fee['TYPE'] = 'fee'
    uphold_fee['AMOUNT'] = uphold_fee['AMOUNT'] * -1

    uphold_bal = pd.concat([uphold_dep,uphold_fee])
    uphold_bal['DATE'] = pd.to_datetime(uphold_bal['DATE'],utc = True)

    
    blockfi = bfi.rename(columns = {
        'Cryptocurrency':'CURRENCY',
        'Amount':'AMOUNT',
        'Transaction Type':'TYPE',
        'Confirmed At':'DATE'
    },inplace = True)
    idx_dep = bfi.index[bfi['TYPE'] == 'Crypto Transfer']
    bfi_dep = bfi.loc[idx_dep]
    bfi_dep['TYPE'] = 'deposit'

    idx_int = bfi.index[bfi['TYPE'] == 'Interest Payment']
    bfi_int = bfi.loc[idx_int]
    bfi_int['TYPE'] = 'interest'

    idx_wth = bfi.index[bfi['TYPE'] == 'BIA Withdraw']
    bfi_wth = bfi.loc[idx_wth]
    bfi_wth['TYPE'] = 'withdrawal'

    bfi = pd.concat([bfi_dep,bfi_int,bfi_wth])
    bfi['DATE'] = pd.to_datetime(bfi['DATE'],utc = True)



    
    wallet = pd.concat([exodus,uphold_bal,nexo,bfi])
    wallet = wallet.drop(['ADDRESS',
                          'TXID',
                          'TXURL'],
                         axis=1)
#     for i in wallet['AMOUNT']:
#     #     print(i)
#         q = wallet.index[wallet['AMOUNT'] == i]
#         wallet['AMOUNT'].loc[q] = (float(i) * random.random()) 

#     variable_list = wallet['CURRENCY'].unique()
#     for variable_name in variable_list:
#         balance = wallet.loc[wallet['CURRENCY'] == variable_name, 'AMOUNT'].astype(float).sum()
#     #     print(variable_name)
#     #     print(balance)
    
    return wallet


def trades():
    pth = '/home/simon/stuff/coding/crypto_dash/data/'
    swan_raw = pd.read_csv('~/stuff/coding/crypto_dash/data/20220313_swan_dep_purch.csv')
    uphold = pd.read_csv(pth + '20220311_uphold.csv')
    cbp_fills_raw = pd.read_csv(pth + '20220311_cbp_fills.csv')
    bittrex_21_raw  = pd.read_csv(pth + '2021_bittrex.csv')
    
    
    
        # Uphold Buys
    idx_buy = (uphold.index[uphold['Destination'] == 'uphold'] & 
               uphold.index[uphold['Origin']=='bank'])
    uphold_buys = uphold.loc[idx_buy]
    uphold_buys.rename(columns = {
        'Destination Amount':'AMOUNT',
        'Destination Currency':'CURRENCY',
        'Origin Amount':'TOTAL',
        'Date':'DATE'
    },inplace=True)

    uphold_buys['PRICE'] = uphold_buys['TOTAL'] / uphold_buys['AMOUNT']
    uphold_buys['TYPE'] = 'BUY'

    # Uphold Sales
    idx_sell = (uphold.index[uphold['Destination'] == 'bank'] & 
               uphold.index[uphold['Origin']=='uphold'])
    uphold_sell = uphold.loc[idx_sell]
    uphold_sell.rename(columns = {
        'Destination Amount':'TOTAL',
        'Origin Currency':'CURRENCY',
        'Origin Amount':'AMOUNT',
        'Date':'DATE'
    },inplace=True)

    uphold_sell['TYPE'] = 'SELL'
    uphold_sell['PRICE'] = uphold_sell['TOTAL'] / uphold_sell['AMOUNT']
    uphold_tx = pd.concat([uphold_buys,uphold_sell])

    for i in uphold_tx['AMOUNT']:
    #     print(i)
        q = uphold_tx.index[uphold_tx['AMOUNT'] == i]
        uphold_tx['AMOUNT'].loc[q] = (float(i) * random.random())
        uphold_tx['TOTAL'] = uphold_tx['AMOUNT'] * uphold_tx['PRICE']

    # uphold_tx['DATE'] = pd.to_datetime(uphold_tx['DATE'],utc=True)
    uphold_tx['ACCOUNT'] = 'Uphold'
    uphold_tx
    uphold_tx = uphold_tx.drop([
        'Id',
        'Status',
        'Origin',
        'Destination',
        'Fee Amount',
        'Fee Currency',
        'Type',
        'Destination Currency',
        'Origin Currency'
    ],axis=1)
    uphold_tx['DATE'] = pd.to_datetime(uphold_tx['DATE'],utc=True)
    uphold_tx.sort_values(by='DATE')

    
    
    cbp = pd.DataFrame(cbp_fills_raw)
    cbp.rename(columns = {'side':'TYPE',
                          'created at':'DATE',
                          'size':'AMOUNT',
                          'size unit':'CURRENCY',
                          'price':'PRICE',
                          },
               inplace = True)
    cbp['DATE'] = pd.to_datetime(cbp['DATE'])
    cbp = cbp.drop([
        'portfolio',
        'trade id',
        'product',
        'price/fee/total unit',
        'total',
        'fee'
    ],axis=1)
    cbp['ACCOUNT'] = 'CBP'
    cbp['DATE'] = pd.to_datetime(cbp['DATE'],utc=True)

    
    
    
    idx = swan_raw.index[swan_raw['Event']=='purchase']
    swan = swan_raw.loc[idx]
    swan.rename(columns = {
        'Event':'TYPE',
        'Date':'DATE',
        'Unit Count':'AMOUNT',
        'Asset Type':'CURRENCY',
        'BTC Price':'PRICE',
        'Fee USD':'FEE',
    },inplace = True)
    swan['TYPE'] = 'BUY'
    swan = swan.drop([
        'Timezone',
        'Status',
        'USD',
        'FEE',
    ],axis = 1)
    swan['ACCOUNT'] = 'Swan'
    swan['DATE'] = pd.to_datetime(swan['DATE'],utc=True)

    
    
    bitt = bittrex_21_raw
    bitt['Exchange'] = bitt['Exchange'].str[4:]
    bitt.rename(columns = {'Exchange':'CURRENCY',
                           'TimeStamp':'DATE',
                           'Quantity':'AMOUNT',
                           'Price':'TOTAL',
                           'PricePerUnit':'PRICE'},
                inplace = True)
    bitt['ACCOUNT'] = 'Bittrex'
    bitt['TYPE'] = 'BUY'
    bitt['DATE'] = pd.to_datetime(bitt['DATE'],utc=True)

    bitt.sort_values(by='DATE')
    bitt = bitt[[
        'TYPE',
        'DATE',
        'AMOUNT',
        'CURRENCY',
        'PRICE',
        'ACCOUNT'
    ]]

    
    trades = pd.concat([cbp,swan,bitt,uphold_tx])
    trades['TOTAL'] = trades['AMOUNT'] * trades['PRICE']
    trades['DATE'] = pd.to_datetime(trades['DATE'],utc=True)
    trades.sort_values(by='DATE')
    
    return trades

    
       