import logging
from typing import Optional, Union, List, Dict
import os
import fredapi as fred
import pandas as pd
from datetime import datetime
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, data_dir: str = "./data", fred_api_key: Optional[str] = None):
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")

        # create directories if they don't exist
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

        # initialize fred api key if provided
        self.fred_client = None
        if fred_api_key:
            try:
                self.fred_client = fred.Fred(api_key=fred_api_key)
            except Exception as e:
                logger.warning(f"Failed to initialize FRED clinet: {e}")
        
    def fetch_stock_data(self,
                   tickers: Union[str, List[str]] = ["^IXIC", "^DJI", "^GSPC"],
                   start_date: str = "2000-04-01",
                   end_date: str = None,
                   interval: str = "1d",
                   save: bool = True) -> Dict[str, pd.DataFrame]:
        
        if end_date == None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        results = {}

        # convert single ticker to list
        if isinstance(tickers, str):
            ticker_list = [tickers]
        else:
            ticker_list = tickers

        # pull data for each ticker in the list
        for ticker in ticker_list:
            try:
                logger.info(f"Fetching data for {ticker}")
                stock_data = yf.download(ticker,
                                         start=start_date,
                                         end=end_date,
                                         interval=interval,
                                         auto_adjust=True,
                                         progress=False)
                
                # sort newest first
                stock_data = stock_data.sort_index(ascending=False)
                
                # save data if specified
                if save:
                    file_name = f"{ticker}_{start_date}_{end_date}.csv"
                    file_path = os.path.join(self.raw_dir, file_name)
                    stock_data.to_csv(file_path)
                    logger.info(f"Saved {ticker} data to {file_path}")
                
                # add stock data to dictionary
                results[ticker] = stock_data

            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")
        
        return results
    
    def fetch_economic_indicators(self,
                                  indicators: Union[str, List[str]] = ["GDP", "CP", "CIVPART"],
                                  start_date: str = "2000-04-01",
                                  end_date: Optional[str] = None,
                                  save: bool = True) -> Dict[str, pd.DataFrame]:
        if self.fred_client == None:
            logger.error(f"FRED client API not initialized. Please provide a valid API key.")
            return {}
        
        if end_date == None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        results = {}

        # convert single indicator to list
        if isinstance(indicators, str):
            indicator_list = [indicators]
        else:
            indicator_list = indicators

        # pull data for each indicator in the list
        for indicator in indicator_list:
            try:
                logger.info(f"Fetching economic indicator: {indicator}")
                series = self.fred_client.get_series(indicator,
                                                     observation_start=start_date,
                                                     observation_end=end_date)
                
                # convert series to dataframe
                df = pd.DataFrame(series)

                # sort newest first
                df = df.sort_index(ascending=False)

                # label data
                df.columns = [indicator]
                df.index.name = "Date"

                if save:
                    file_name = f"{indicator}_{start_date}_{end_date}.csv"
                    file_path = os.path.join(self.raw_dir, file_name)
                    df.to_csv(file_path)
                    logger.info(f"Saved {indicator} data to {file_path}")

                # add economic indicator to dictionary
                results[indicator] = df

            except Exception as e:
                logger.error(f"Error fetching {indicator}: {e}")
        
        return results
