import yfinance as yf
import pandas as pd 
from datetime import datetime,timedelta
from model.database import StockData,get_DaysStockData
from sqlalchemy import create_engine
import time
import talib



def get_nifty500_symbols():
    # Need to fetech the data from csv file.
    df =pd.read_csv("ind_nifty500list.csv")
    symbols = df['Symbol'].tolist()
    return [symbol + ".NS" for symbol in symbols]



def getSpecificStockData(symbol,table_name,interval):
    stock = yf.Ticker(symbol)

     # get price data
    endDate = datetime.now()
    startTime = endDate - timedelta(days=1500) # get 50 days of data
    priceData = stock.history(start=startTime,end=endDate,interval=interval)

    if priceData.empty:
        return None
    
    priceData['Symbol'] = symbol
    # daily stock info
    buyLowSellHigh(priceData)
   

    engine=create_engine('postgresql://root:secret@localhost:5432/stock')

    dropColumnAndSaveSql = priceData.drop("Stock Splits",axis=1)
    catchDate = dropColumnAndSaveSql.reset_index()

    catchDate['Date'] = pd.to_datetime(catchDate['Date']).dt.date
    
    updateData = catchDate.iloc[[-1]]  #using a table square breaket wouldn't change the object type    
    
    table_name = table_name
    try:
        updateData.to_sql(table_name,engine,if_exists='append', index=False)
        print(f"Data successfully inserted/appended to table '{table_name}'.")
    except Exception as e:
        print(f"An error occurred during database operation: {e}")

    # save this data and use that save data to update the 
    # Stock fundamental
    # get_stock_data(stock=stock,priceData=priceData,symbol=symbol)
  

def buyLowSellHigh(priceData):
    length = 25
    profitPercentage = 5

    # Calculate the 25 days low
    priceData['Low25'] = priceData['Low'].rolling(window=length,min_periods=1).min()

    # Calcualte GTT (6.5% above the LOW25)
    priceData['Gtt']= priceData['Low25'] * 1.065

    # Calculate Target (5% above the GTT)
    priceData['Target'] = priceData['Gtt'] * 1.05


    # Calculate single day volume   
    priceData['Daily_Volume'] = priceData['Volume'].pct_change() * 100

    # Calculate 3-day volume change percentage
    priceData['ThreeDay_Volume'] = (
        priceData['Volume'].rolling(window=3).apply(lambda x: (x[-1] - x[0]) / x[0] * 100 if x[0] != 0 else 0, raw=True)
    )

    # Calculate weekly (7-day) volume change percentage
    priceData['Weekly_Volume'] = (
        priceData['Volume'].rolling(window=7).apply(lambda x: (x[-1] - x[0]) / x[0] * 100 if x[0] != 0 else 0, raw=True)
    )
    # Sma
    priceData['Sma20'] = talib.SMA(priceData['Close'],timeperiod=20)
    priceData['Sma50'] = talib.SMA(priceData['Close'],timeperiod=50)
    priceData['Sma200'] = talib.SMA(priceData['Close'],timeperiod=200)
    # Rsi
    priceData['Rsi'] = talib.RSI(priceData['Close'],timeperiod=14)

    priceData['Macd'],priceData['Macd_Signal'],priceData["Macd_Hist"] = talib.MACD(priceData['Close'])

    priceData['BB_Upper'],priceData['BB_Middle'],priceData["BB_Lower"] = talib.BBANDS(priceData['Close'])


    priceData['Obv'] = talib.OBV(priceData['Close'],priceData['Volume'])

    priceData['Analysis'] = get_Signals(priceData)


def get_stock_data(stock,priceData,symbol):
   
    if priceData.empty:
        return None
    
    # priceData.from_dict()
    latest_price = priceData.iloc[-1]

    # Daily Change
    daily_change = ((latest_price['Close'] - priceData.iloc[-2]['Close']) / priceData.iloc[-2]['Close']) * 100

    # Weekly Change
    weekly_change = ((latest_price['Close'] - priceData.iloc[-6]['Close']) / priceData.iloc[-6]['Close']) * 100

    # Monthly Change (assuming 22 trading days in a month)
    monthly_change = ((latest_price['Close'] - priceData.iloc[-22]['Close']) / priceData.iloc[-22]['Close']) * 100


    # Get fundamental of data
    info = stock.info
    stockData = {
    'Symbol': symbol,
    'Company Name': info.get('longName', 'N/A'),
    'Sector': info.get('sector', 'N/A'),
    'Industry': info.get('industry', 'N/A'),
    'Market Cap': info.get('marketCap', 'N/A'),
    'P/E Ratio': info.get('trailingPE', 'N/A'),
    'Forward P/E': info.get('forwardPE', 'N/A'),
    'PEG Ratio': info.get('pegRatio', 'N/A'),
    'Price to Book': info.get('priceToBook', 'N/A'),
    'EV/EBITDA': info.get('enterpriseToEbitda', 'N/A'),
    'Profit Margin': info.get('profitMargins', 'N/A'),
    'Operating Margin': info.get('operatingMargins', 'N/A'),
    'ROE': info.get('returnOnEquity', 'N/A'),
    'ROA': info.get('returnOnAssets', 'N/A'),
    'Revenue': info.get('totalRevenue', 'N/A'),
    'Revenue Per Share': info.get('revenuePerShare', 'N/A'),
    'Quarterly Revenue Growth': info.get('quarterlyRevenueGrowth', 'N/A'),
    'Gross Profit': info.get('grossProfits', 'N/A'),
    'EBITDA': info.get('ebitda', 'N/A'),
    'Net Income': info.get('netIncomeToCommon', 'N/A'),
    'EPS': info.get('trailingEps', 'N/A'),
    'Quarterly Earnings Growth': info.get('quarterlyEarningsGrowth', 'N/A'),
    'Total Cash': info.get('totalCash', 'N/A'),
    'Total Debt': info.get('totalDebt', 'N/A'),
    'Debt To Equity': info.get('debtToEquity', 'N/A'),
    'Current Ratio': info.get('currentRatio', 'N/A'),
    'Book Value': info.get('bookValue', 'N/A'),
    'Free Cash Flow': info.get('freeCashflow', 'N/A'),
    'Dividend Rate': info.get('dividendRate', 'N/A'),
    'Dividend Yield': info.get('dividendYield', 'N/A'),
    'Payout Ratio': info.get('payoutRatio', 'N/A'),
    'Beta': info.get('beta', 'N/A'),
    '52 Week High': info.get('fiftyTwoWeekHigh', 'N/A'),
    '52 Week Low': info.get('fiftyTwoWeekLow', 'N/A'),
    '50 Days Average': info.get('fiftyDayAverage', 'N/A'),
    '200 Days Average': info.get('twoHundredDayAverage', 'N/A'),
    'Latest Price': latest_price.get('Close', 'N/A'),
    'Volume': latest_price.get('Volume', 'N/A')
    
   

}
    # print(stockData)
    # TODO: Inset fundamental data here.=> remove return
    return stockData


def callFetecher():
    nifty500 = get_nifty500_symbols()
    Weeklyinterval='1wk'
    weeklyStockTable = "stock_data_weekly"
    OneDayinterval='1d'
    dasyStockTable = "stock_data"
    for stock in nifty500:
        print(f"Stock data update:{stock}")
        getSpecificStockData(stock,weeklyStockTable,Weeklyinterval)
        print("completed")
       

def singleFetecher():
    getSpecificStockData("GVT&D.NS")




# Todo: get the days datea 
def previous200DaysData(symbol):
    listOfStock = get_nifty500_symbols()
    
    result = get_DaysStockData(stock_symbol=symbol,limit=1)
    stock_dicts = []
    for stock in result:
        stock_dicts.append({
            "Date": stock.Date,
            "Symbol": stock.Symbol,
            "Open": stock.Open,
            "High": stock.High,
            "Low": stock.Low,
            "Close": stock.Close,
            "Volume": stock.Volume,
            "Low25": stock.Low25,
            "Gtt": stock.Gtt,
            "Target": stock.Target,
        })
    # for re in stock_dicts:
    last_updated_date = stock_dicts[0]['Date']
   
    df = pd.to_datetime(last_updated_date)
    cur = pd.to_datetime(datetime.now().date())

    diff_days = (cur-df).days 
    
    return diff_days

def get_Signals(data):
    signals = []

    # Trend Analysis (Moving Average Crossover)
    if pd.notnull(data['Sma20']).all() and pd.notnull(data['Sma50']).all():
        if (data['Sma20'] > data['Sma50']).all():
            signals.append("Bullish MA Crossover")
        else:
            signals.append("Bearish MA Crossover")

    # RSI Analysis
    if pd.notnull(data['Rsi']).all():
        if (data['Rsi'] > 70).all():
            signals.append("Overbought (Rsi)")
        elif (data['Rsi'] < 30).all():
            signals.append("Oversold (Rsi)")

    # MACD Analysis
    if pd.notnull(data['Macd']).all() and pd.notnull(data['Macd_Signal']).all():
        if (data['Macd'] > data['Macd_Signal']).all():
            signals.append("Bullish MACD")
        else:
            signals.append("Bearish MACD")

    # Bollinger Bands Analysis
    if pd.notnull(data['BB_Upper']).all() and pd.notnull(data['BB_Lower']).all() and pd.notnull(data['Close']).all():
        if (data['Close'] > data['BB_Upper']).all():
            signals.append("Overbought (BB)")
        elif (data['Close'] < data['BB_Lower']).all():
            signals.append("Oversold (BB)")

    # Return the signals as a string
    return "; ".join(signals) if signals else "No clear signals"

