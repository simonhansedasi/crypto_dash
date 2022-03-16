import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import random


#start with exodus wallet
exo = pd.read_csv("/home/simon/stuff/coding/crypto_dash/data/20220311_exodus.csv",
                  na_filter=False,
                  delimiter=',', quotechar='"')
#to more easily track txid extract into different datasets and merge later. 
in_idx = exo.index[exo["OUTTXID"]==""]
exo_in = exo.loc[in_idx]
exo_in = exo_in.drop("FROMPORTFOLIO", axis=1)
exo_in = exo_in.drop("TOPORTFOLIO", axis=1)
exo_in = exo_in.drop("OUTAMOUNT", axis=1)
exo_in = exo_in.drop("OUTCURRENCY", axis=1)
exo_in = exo_in.drop("OUTTXID", axis=1)
exo_in = exo_in.drop("OUTTXURL", axis=1)
exo_in = exo_in.drop("ORDERID", axis=1)
exo_in = exo_in.drop("PERSONALNOTE", axis=1)
exo_in = exo_in.drop("FEEAMOUNT", axis=1)
exo_in = exo_in.drop("FEECURRENCY", axis=1)
exo_in.rename(columns = {"INAMOUNT":"AMOUNT",
                          "INCURRENCY":"CURRENCY",
                          "TOADDRESS":"ADDRESS",
                          "INTXID":"TXID",
                          "INTXURL":"TXURL"},
           inplace = True)

out_idx = exo.index[exo["INTXID"]==""]
exo_out = exo.loc[out_idx]
exo_out = exo_out.drop("TOPORTFOLIO", axis=1)
exo_out = exo_out.drop("INAMOUNT", axis=1)
exo_out = exo_out.drop("INCURRENCY", axis=1)
exo_out = exo_out.drop("INTXID", axis=1)
exo_out = exo_out.drop("INTXURL", axis=1)
exo_out = exo_out.drop("ORDERID", axis=1)
exo_out = exo_out.drop("PERSONALNOTE", axis=1)
exo_out = exo_out.drop("FROMPORTFOLIO", axis=1)
exo_out = exo_out.drop("FEEAMOUNT", axis=1)
exo_out = exo_out.drop("FEECURRENCY", axis=1)


exo_out.rename(columns = {"OUTAMOUNT":"AMOUNT",
                          "OUTCURRENCY":"CURRENCY",
                          "TOADDRESS":"ADDRESS",
                          "OUTTXID":"TXID",
                          "OUTTXURL":"TXURL"},
           inplace = True)

fee_idx = exo.index[exo["FEEAMOUNT"] !=""]
exo_fee = exo.loc[fee_idx]
exo_fee = exo_fee.drop("FROMPORTFOLIO",axis=1)
exo_fee = exo_fee.drop("TOPORTFOLIO",axis=1)
exo_fee = exo_fee.drop("INAMOUNT",axis=1)
exo_fee = exo_fee.drop("INCURRENCY",axis=1)
exo_fee = exo_fee.drop("INTXID",axis=1)
exo_fee = exo_fee.drop("INTXURL",axis=1)
exo_fee = exo_fee.drop("ORDERID",axis=1)
exo_fee = exo_fee.drop("PERSONALNOTE",axis=1)
exo_fee = exo_fee.drop("OUTAMOUNT",axis=1)
exo_fee = exo_fee.drop("OUTCURRENCY",axis=1)
exo_fee = exo_fee.drop("TYPE",axis=1)
exo_fee["TYPE"]="fee"
exo_fee.rename(columns = {"FEEAMOUNT":"AMOUNT",
                          "FEECURRENCY":"CURRENCY",
                          "TOADDRESS":"ADDRESS",
                          "OUTTXID":"TXID",
                          "OUTTXURL":"TXURL"},
           inplace = True)

exodus = pd.concat([exo_in,exo_out,exo_fee],ignore_index=True)
# exo_test["DATE"] = pd.to_datetime(exo_test["DATE"])

#data scrambler
for i in exodus["AMOUNT"]:
    q = exodus.index[exodus["AMOUNT"] == i]    
    exodus["AMOUNT"].loc[q] = (float(i) * random.random())
    
#remove first four characters from date
exodus["DATE"] = exodus["DATE"].str[4:]
#remove last n characters from date
exodus["DATE"] = exodus["DATE"].str[:-33]   
exodus["DATE"] = pd.to_datetime(exodus["DATE"])

# print(exodus)
exodus.head()

#exodus balances
# variable_list = exodus["CURRENCY"].unique()
# for variable_name in variable_list:
#     balance = exodus.loc[exodus["CURRENCY"] == variable_name, "AMOUNT"].astype(float).sum()
#     print(variable_name)
#     print(balance)