import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote

# names=['ACC IB Equity', 'ACEM IB Equity','ADANI IB Equity', 'ADSEZ IB Equity', 'AL IB Equity', 'APHS IB Equity', 'APNT IB Equity', 'ARBP IB Equity', 'AUBANK IB Equity', 'AXSB IB Equity', 'BAF IB Equity', 'BANDHAN IB Equity', 'BHARTI IB Equity', 'BHE IB Equity', 'BHFC IB Equity', 'BJAUT IB Equity', 'BJFIN IB Equity', 'BOB IB Equity', 'BPCL IB Equity', 'BRIT IB Equity', 'CIPLA IB Equity', 'CLGT IB Equity', 'COAL IB Equity', 'DABUR IB Equity', 'DIVI IB Equity', 'DLFU IB Equity', 'DMART IB Equity', 'DRRD IB Equity', 'EIM IB Equity', 'FB IB Equity', 'GAIL IB Equity', 'GCPL IB Equity', 'GPL IB Equity', 'GRASIM IB Equity', 'HAVL IB Equity', 'HCLT IB Equity', 'HDFCB IB Equity', 'HMCL IB Equity', 'HNDL IB Equity', 'HUVR IB Equity', 'ICICIBC IB Equity', 'ICICIGI IB Equity', 'IIB IB Equity', 'INDIGO IB Equity', 'INDUSTOW IB Equity', 'INFO IB Equity', 'INFOE IB Equity', 'IOCL IB Equity', 'IPRU IB Equity', 'IRCTC IB Equity', 'ITC IB Equity', 'JSTL IB Equity', 'JUBI IB Equity', 'KMB IB Equity', 'LPC IB Equity', 'LT IB Equity', 'LTIM IB Equity', 'MAXF IB Equity', 'MM IB Equity', 'MPHL IB Equity', 'MRCO IB Equity', 'MSIL IB Equity', 'NEST IB Equity', 'NTPC IB Equity', 'ONGC IB Equity', 'PAG IB Equity', 'PI IB Equity', 'PIDI IB Equity', 'PWGR IB Equity', 'RIL IB Equity', 'SBICARD IB Equity', 'SBILIFE IB Equity', 'SBIN IB Equity', 'SIEM IB Equity', 'SRCM IB Equity', 'SRF IB Equity', 'SUNP IB Equity', 'TATA IB Equity', 'TATACONS IB Equity', 'TCS IB Equity', 'TECHM IB Equity', 'TPWR IB Equity', 'TRENT IB Equity', 'TTAN IB Equity', 'TTMT IB Equity', 'TTMT/A IB Equity', 'UPLL IB Equity', 'UTCEM IB Equity', 'VEDL IB Equity', 'VOLT IB Equity', 'WPRO IB Equity', 'Z IB Equity']

#------------------------------------------------------------------------------------------------------#
                            # This part is used to fetch the cnsx symbol #

def Fetch_Company_Name(eq):
  url='https://www.google.com/search?q=site:Bloomberg.com '+eq.split()[0]+':IN'
  res=requests.get(url)
  name=None
  if res.status_code==200:
    soup=BeautifulSoup(res.content, 'html.parser')
    links=[m.text for m in soup.find_all('a')]
    for j in links:
      if j.endswith(f'{eq.split()[0]}:IN') and 'Ltd' in j:
        name=j.split('Ltd')[0]+"Ltd"
      if not name and j.endswith(f'{eq.split()[0]}:IN') and 'Stock Price Quote' in j:
        name=j.split(' Stock Price Quote')[0]
      if name and  ': ' in name:
        name=name.split(': ')[1]
  return name

def Fetch_original_name(name):
  url='https://www.google.com/search?q=site%3Awww.google.com%2Ffinance+'+f"{quote(name)}"
  res=requests.get(url)
  if res.status_code==200:
    soup=BeautifulSoup(res.content, 'html.parser')
    links=[m.text for m in soup.find_all('div', {'class':'BNeawe UPmit AP7Wnd lRVwie'})]
    for i in links:
      if i.endswith(':NSE'):
        return(i.split()[-1].split(':')[0])

def Fetch_CNSX(eq):
  name=Fetch_Company_Name(eq)
  url=f'http://www.google.com/finance?&q={name}'
  headers={'User-Agent': 'Mozilla/5.0'}
  resp=requests.get(url,headers=headers)
  title=BeautifulSoup(resp.content, 'html.parser')
  try:
    title=title.find('div',{'class':'PdOqHc'}).contents[-1].split(' • NSE')[0]
  except:
    title=Fetch_original_name(name)
  return title+'.NS'


#--------------------------------------------------------------------------------------------------------------------#
                                  # Thsi part is used to download the data #

def get_date(d):
    date_object=datetime.strptime(d, '%Y-%m-%d')
    while date_object.strftime('%A')=='Saturday' or date_object.strftime('%A')=='Sunday':
        date_object-=timedelta(days=1)
    return date_object.strftime('%Y-%m-%d')

def stocks_for_each_year(stock_name,f,t):
    data_list=[]
    for i in range(f,t+1):
        date=f'{i}-12-31'
        date=get_date(date)
        date_object = datetime.strptime(date, '%Y-%m-%d')+ timedelta(days=1)
        new_date= date_object.strftime('%Y-%m-%d')
        tickerData = yf.Ticker(stock_name)
        data = tickerData.history(start=date, end=new_date)
        data=pd.DataFrame(data)
        data_list.append(data)
    data_list=pd.concat(data_list)
    return data_list

def stocks_freq(stock_name,f,t,freq):
    data_list=[]
    for i in range(f,t+1):
        start_date=f'{i}-01-01'
        end_date=f'{i}-12-31'
        tickerData = yf.Ticker(stock_name)
        data = tickerData.history(start=start_date, end=end_date, interval=freq)
        data=pd.DataFrame(data)
        data_list.append(data)
    data_list=pd.concat(data_list)
    return data_list

def process_stock(stock,name, f, t, freq):
    if freq=='1Y':
        x = stocks_for_each_year(stock, f,t)
    else:
        x=stocks_freq(stock,f,t,freq)
    x.columns = pd.MultiIndex.from_product([[name], x.columns])
    print(name)
    return x

def display_dataframe(symbol,name, from_date, to_date, freq):
    print("Processed:")
    final_dataframe = process_stock(symbol, name, from_date, to_date, freq)
    final_dataframe.index = final_dataframe.index.tz_convert(None)
    final_dataframe.to_excel(f'{name}.xlsx')
    print(f'Downloaded {name}.xlsx')
    return final_dataframe




