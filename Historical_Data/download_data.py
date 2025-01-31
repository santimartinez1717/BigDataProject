from TradingviewData import TradingViewData,Interval


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
    "Stellar": "XLMUSDs", 
}


def download_data_for_cryptos():
    tv = TradingViewData()  
    for crypto_name, symbol in cryptos.items():
        print(f"Obteniendo datos para {crypto_name}...")
        data = tv.get_hist(symbol, exchange="COINBASE", interval=Interval.daily, n_bars=4*365)
        data.to_csv(f"{crypto_name}_historical_data.csv")  # Guardar los datos como archivo CSV
        print(f"Datos de {crypto_name} guardados exitosamente.")



if __name__ == "__main__":
    download_data_for_cryptos()