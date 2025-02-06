from TradingviewData import TradingViewData,Interval
import os


cryptos = {
    "Bitcoin": "BTCUSD",  
    "Ethereum": "ETHUSD", 
    "Ripple": "XRPUSD",  
    "Solana": "SOLUSD",  
    "Dogecoin": "DOGEUSD",
    "Cardano": "ADAUSD",  
    "Shiba Inu": "SHIBUSD", 
    "Polkadot": "DOTUSD",  
    "Aave": "AAVEUSD",  
    "Stellar": "XLMUSD", 
}

def download_data_for_cryptos():
    if not os.path.exists("data"):
        os.makedirs("data")

    tv = TradingViewData()  
    for crypto_name, symbol in cryptos.items():
        print(f"Obteniendo datos para {crypto_name}...")
        data = tv.get_hist(symbol, exchange="COINBASE", interval=Interval.daily, n_bars=4*365)

        lenght_new_datasets = 365
        lenght_dataset = len(data)
        data_years = [(data[i:i+lenght_new_datasets],data.index[i].year) for i in range(0,lenght_dataset,lenght_new_datasets)]

        for data_year in data_years:
            data_year[0].to_csv(f"data/{crypto_name}_historical_data_{data_year[1]}.csv")  
        print(f"Datos de {crypto_name} guardados exitosamente.")

if __name__ == "__main__":
    download_data_for_cryptos()