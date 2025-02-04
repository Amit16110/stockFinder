from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func,select,desc

db = SQLAlchemy()

class StockData(db.Model):
    __tablename__ = 'stock_data'

    Date = db.Column(db.Date, primary_key=True, nullable=False)
    Symbol = db.Column(db.String(50), primary_key=True, nullable=False)
    Open = db.Column(db.Numeric(50, 2))
    High = db.Column(db.Numeric(50, 2))
    Low = db.Column(db.Numeric(50, 2))
    Close = db.Column(db.Numeric(50, 2))
    Volume = db.Column(db.BigInteger)
    Dividends = db.Column(db.Numeric(50, 2))
    Stock_splits = db.Column(db.Numeric(50, 2))
    Low25 = db.Column(db.Numeric(50, 2))
    Gtt = db.Column(db.Numeric(50, 2))
    Target = db.Column(db.Numeric(50, 2))
    Daily_Volume = db.Column(db.BigInteger)
    ThreeDay_Volume = db.Column(db.BigInteger)
    Weekly_Volume = db.Column(db.BigInteger)
    Sma20 = db.Column(db.Numeric(50, 2))
    Sma50 = db.Column(db.Numeric(50, 2))
    Sma200 = db.Column(db.Numeric(50, 2))
    Rsi = db.Column(db.Numeric(50, 2))
    Macd = db.Column(db.Numeric(50, 2))
    Macd_Signal = db.Column(db.Numeric(50, 2))
    Macd_Hist = db.Column(db.Numeric(50, 2))
    BB_Upper = db.Column(db.Numeric(50, 2))
    BB_Middle = db.Column(db.Numeric(50, 2))
    BB_Lower = db.Column(db.Numeric(50, 2))
    Obv = db.Column(db.Numeric(50, 2))
    Analysis = db.Column(db.String(200))

    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now())  # Auto-set when created
    updated_at = db.Column(db.DateTime(timezone=True), onupdate=func.now())  # Auto-update when modified
    def __repr__(self):
        return (f"<StockData(Date={self.Date}, Symbol='{self.Symbol}', Open={self.Open}, "
                f"High={self.High}, Low={self.Low}, Close={self.Close}, Volume={self.Volume}, "
                f"Dividends={self.Dividends}, Stock_splits={self.Stock_splits}, Low25={self.Low25}, "
                f"Gtt={self.Gtt}, Target={self.Target}, Daily_Volume={self.Daily_Volume}, "
                f"ThreeDay_Volume={self.ThreeDay_Volume}, Weekly_Volume={self.Weekly_Volume}, "
                f"created_at={self.created_at}, updated_at={self.updated_at})>")

class StockDataWeakly(db.Model):
    __tablename__ = 'stock_data_weekly'

    Date = db.Column(db.Date, primary_key=True, nullable=False)
    Symbol = db.Column(db.String(50), primary_key=True, nullable=False)
    Open = db.Column(db.Numeric(50, 2))
    High = db.Column(db.Numeric(50, 2))
    Low = db.Column(db.Numeric(50, 2))
    Close = db.Column(db.Numeric(50, 2))
    Volume = db.Column(db.BigInteger)
    Dividends = db.Column(db.Numeric(50, 2))
    Stock_splits = db.Column(db.Numeric(50, 2))
    Low25 = db.Column(db.Numeric(50, 2))
    Gtt = db.Column(db.Numeric(50, 2))
    Target = db.Column(db.Numeric(50, 2))
    Daily_Volume = db.Column(db.BigInteger)
    ThreeDay_Volume = db.Column(db.BigInteger)
    Weekly_Volume = db.Column(db.BigInteger)
    Sma20 = db.Column(db.Numeric(50, 2))
    Sma50 = db.Column(db.Numeric(50, 2))
    Sma200 = db.Column(db.Numeric(50, 2))
    Rsi = db.Column(db.Numeric(50, 2))
    Macd = db.Column(db.Numeric(50, 2))
    Macd_Signal = db.Column(db.Numeric(50, 2))
    Macd_Hist = db.Column(db.Numeric(50, 2))
    BB_Upper = db.Column(db.Numeric(50, 2))
    BB_Middle = db.Column(db.Numeric(50, 2))
    BB_Lower = db.Column(db.Numeric(50, 2))
    Obv = db.Column(db.Numeric(50, 2))
    Analysis = db.Column(db.String(200))

    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now())  # Auto-set when created
    updated_at = db.Column(db.DateTime(timezone=True), onupdate=func.now())  # Auto-update when modified
    def __repr__(self):
        return (f"<StockData(Date={self.Date}, Symbol='{self.Symbol}', Open={self.Open}, "
                f"High={self.High}, Low={self.Low}, Close={self.Close}, Volume={self.Volume}, "
                f"Dividends={self.Dividends}, Stock_splits={self.Stock_splits}, Low25={self.Low25}, "
                f"Gtt={self.Gtt}, Target={self.Target}, Daily_Volume={self.Daily_Volume}, "
                f"ThreeDay_Volume={self.ThreeDay_Volume}, Weekly_Volume={self.Weekly_Volume}, "
                f"created_at={self.created_at}, updated_at={self.updated_at})>")


class StockFundamentals(db.Model):
    __tablename__ = 'stock_fundamentals'

    symbol = db.Column(db.String(50), primary_key=True, nullable=False)
    company_name = db.Column(db.String(200))
    sector = db.Column(db.String(100))
    industry = db.Column(db.String(100))
    market_cap = db.Column(db.BigInteger)
    pe_ratio = db.Column(db.Float)
    forward_pe = db.Column(db.Float)
    peg_ratio = db.Column(db.Float)
    price_to_book = db.Column(db.Float)
    ev_ebitda = db.Column(db.Float)
    profit_margin = db.Column(db.Float)
    operating_margin = db.Column(db.Float)
    roe = db.Column(db.Float)  # Return on Equity
    roa = db.Column(db.Float)  # Return on Assets
    revenue = db.Column(db.BigInteger)
    revenue_per_share = db.Column(db.Float)
    quarterly_revenue_growth = db.Column(db.Float)
    gross_profit = db.Column(db.BigInteger)
    ebitda = db.Column(db.BigInteger)
    net_income = db.Column(db.BigInteger)
    eps = db.Column(db.Float)  # Earnings per Share
    quarterly_earnings_growth = db.Column(db.Float)
    total_cash = db.Column(db.BigInteger)
    total_debt = db.Column(db.BigInteger)
    debt_to_equity = db.Column(db.Float)
    current_ratio = db.Column(db.Float)
    book_value = db.Column(db.Float)
    free_cash_flow = db.Column(db.BigInteger)
    dividend_rate = db.Column(db.Float)
    dividend_yield = db.Column(db.Float)
    payout_ratio = db.Column(db.Float)
    beta = db.Column(db.Float)
    fifty_two_week_high = db.Column(db.Float)
    fifty_two_week_low = db.Column(db.Float)
    daily_change=db.Column(db.BigInteger)
    weekly_change=db.Column(db.BigInteger)
    monthly_change=db.Column(db.BigInteger)
    # fifty_day_average = db.Column(db.Float)
    # two_hundred_day_average = db.Column(db.Float)
    # latest_price = db.Column(db.Float)
    # rsi = db.Column(db.Float)
    # macd = db.Column(db.Float)
    # signal = db.Column(db.Float)
    # low25 = db.Column(db.Float)
    # gtt = db.Column(db.Float)
    # volume = db.Column(db.BigInteger)
    # Daily_Volume = db.Column(db.BigInteger)
    # threeDay_Volume = db.Column(db.BigInteger)
    # Weekly_Volume = db.Column(db.BigInteger)
    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now())  # Auto-set when created
    updated_at = db.Column(db.DateTime(timezone=True), onupdate=func.now())  # Auto-update when modified


def get_DaysStockData(limit: int):
    result = StockData.query.filter(
        StockData.Date <= func.current_date(),
        # StockData.Symbol == stock_symbol
    ).order_by(StockData.Date.desc()).limit(limit=limit).all()
    return result

def insert_data(stockData):
    try:
        db.session.add(stockData)
        db.session.commit()
        print("Added successfully")
    except Exception as e:
        # db.session.rollback()
        print(f"ERROR:{e}")
