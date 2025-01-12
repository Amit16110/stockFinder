import yfinance as yf
import pandas as pd 
from datetime import datetime,timedelta
from model.database import StockData,get_DaysStockData,StockFundamentals,insert_data
from sqlalchemy import create_engine
import time
import talib


def get_nifty500_symbols():
    # Need to fetech the data from csv file.
    df =pd.read_csv("ind_nifty500list.csv")
    symbols = df['Symbol'].tolist()
    return [symbol + ".NS" for symbol in symbols]



def getSpecificStockData(symbol,table_name,interval,lastDay):
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
    # FOR FUNDAMENTAL.
    # get_stock_data(stock=stock, priceData=priceData,symbol=symbol)
   
# Start
    engine=create_engine('postgresql://root:secret@localhost:5432/stock')

    dropColumnAndSaveSql = priceData.drop("Stock Splits",axis=1)
    catchDate = dropColumnAndSaveSql.reset_index()

    catchDate['Date'] = pd.to_datetime(catchDate['Date']).dt.date
    
    updateData = catchDate.iloc[-lastDay:]  #using a table square breaket wouldn't change the object type    
    
    table_name = table_name
    try:
        updateData.to_sql(table_name,engine,if_exists='append', index=False)
        print(f"Data successfully inserted/appended to table '{table_name}'.")
    except Exception as e:
        print(f"An error occurred during database operation: {e}")

    # save this data and use that save data to update the 
    # Stock fundamental
    # get_stock_data(stock=stock,priceData=priceData,symbol=symbol)
    # # End
  

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
    if len(priceData) >= 6:
        weekly_change = ((latest_price['Close'] - priceData.iloc[-6]['Close']) / priceData.iloc[-6]['Close']) * 100
    else:
        weekly_change = None  # or some defaujlt value

    # Monthly Change (assuming 22 trading days in a month)
    if len(priceData) >= 22:
        monthly_change = ((latest_price['Close'] - priceData.iloc[-22]['Close']) / priceData.iloc[-22]['Close']) * 100
    else:
        monthly_change = None


    # Get fundamental of data
    info = stock.info

 

    stockData = {
        'symbol': clean_data(symbol),
        'company_name': clean_data(info.get('longName', 'N/A')),
        'sector': clean_data(info.get('sector', 'N/A')),
        'industry': clean_data(info.get('industry', 'N/A')),
        'market_cap': clean_data(info.get('marketCap', 'N/A')),
        'pe_ratio': clean_data(info.get('trailingPE', 'N/A')),
        'forward_pe': clean_data(info.get('forwardPE', 'N/A')),
        'peg_ratio': clean_data(info.get('pegRatio', 'N/A')),
        'price_to_book': clean_data(info.get('priceToBook', 'N/A')),
        'ev_ebitda': clean_data(info.get('enterpriseToEbitda', 'N/A')),
        'profit_margin': clean_data(info.get('profitMargins', 'N/A')),
        'operating_margin': clean_data(info.get('operatingMargins', 'N/A')),
        'roe': clean_data(info.get('returnOnEquity', 'N/A')),
        'roa': clean_data(info.get('returnOnAssets', 'N/A')),
        'revenue': clean_data(info.get('totalRevenue', 'N/A')),
        'revenue_per_share': clean_data(info.get('revenuePerShare', 'N/A')),
        'quarterly_revenue_growth': clean_data(info.get('quarterlyRevenueGrowth', 'N/A')),
        'gross_profit': clean_data(info.get('grossProfits', 'N/A')),
        'ebitda': clean_data(info.get('ebitda', 'N/A')),
        'net_income': clean_data(info.get('netIncomeToCommon', 'N/A')),
        'eps': clean_data(info.get('trailingEps', 'N/A')),
        'quarterly_earnings_growth': clean_data(info.get('quarterlyEarningsGrowth', 'N/A')),
        'total_cash': clean_data(info.get('totalCash', 'N/A')),
        'total_debt': clean_data(info.get('totalDebt', 'N/A')),
        'debt_to_equity': clean_data(info.get('debtToEquity', 'N/A')),
        'current_ratio': clean_data(info.get('currentRatio', 'N/A')),
        'book_value': clean_data(info.get('bookValue', 'N/A')),
        'free_cash_flow': clean_data(info.get('freeCashflow', 'N/A')),
        'dividend_rate': clean_data(info.get('dividendRate', 'N/A')),
        'dividend_yield': clean_data(info.get('dividendYield', 'N/A')),
        'payout_ratio': clean_data(info.get('payoutRatio', 'N/A')),
        'beta': clean_data(info.get('beta', 'N/A')),
        'fifty_two_week_high': clean_data(info.get('fiftyTwoWeekHigh', 'N/A')),
        'fifty_two_week_low': clean_data(info.get('fiftyTwoWeekLow', 'N/A')),
        'daily_change': clean_data(daily_change),
        'weekly_change': clean_data(weekly_change),
        'monthly_change': clean_data(monthly_change),
    }

    
    print(stockData)
    newFunda = StockFundamentals(**stockData)

    insert_data(newFunda)
    # TODO: Inset fundamental data here.=> remove return
    return stockData

def clean_data(value):
    if value == 'N/A' or value is None:
        return None
    return value
def callFetecher():
    nifty500 = get_nifty500_symbols()
    Weeklyinterval='1wk'
    weeklyStockTable = "stock_data_weekly"
    OneDayinterval='1d'
    OneDayStockTable = "stock_data"

    
    lastUpdateDay = last_updated_date()

    for stock in nifty500:
        print(f"Stock data update:{stock}")
        getSpecificStockData(stock,OneDayStockTable,OneDayinterval,lastUpdateDay)
        print("completed")
       


# Last stock market data update 
def last_updated_date():
    result = get_DaysStockData(limit=1)
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
    
    if len(stock_dicts) == 0:
        return 3 
    # for re in stock_dicts:
    last_updated_date = stock_dicts[0]['Date']
   
    prev = pd.to_datetime(last_updated_date)
    cur = pd.to_datetime(datetime.now().date())

    diff_days = (cur-prev).days -1
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

