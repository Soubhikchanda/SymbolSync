from final_product import Fetch_CNSX, display_dataframe


# GIVEN BLOOM BERG ITER: 'ACC IB Equity'
name='ACC IB Equity'
symbol=Fetch_CNSX(name) #FETCHING THE CNSX SYMBOL FROM THE BLOOMBERG SYMBOL
from_date=2007
to_date=2022
Freq='1D'

print(display_dataframe(symbol=symbol, name=name, from_date=from_date, to_date=to_date, freq=Freq)) # PRINTS AND SAVES A XLSX FILE OF THR DATAFRAME FOR THE STOCK SYMBOL.