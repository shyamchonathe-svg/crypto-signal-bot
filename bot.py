import os
import time
import ccxt
import pandas as pd
from datetime import datetime
import requests
from typing import List, Dict
import logging
from threading import Thread
from flask import Flask

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Flask app for keeping Render awake
app = Flask(__name__)

@app.route('/')
def home():
    return "🚀 Crypto Signal Bot is running!"

@app.route('/health')
def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.route('/status')
def status():
    return {
        "bot": "Crypto Signal Bot",
        "status": "running",
        "exchange": "Coinbase",
        "timeframe": os.getenv('TIMEFRAME', '1h'),
        "monitoring": "Top 5 coins by volume",
        "signal_type": "EMA(10) x EMA(20) crossovers"
    }

class CryptoSignalBot:
    def __init__(self):
        # Environment variables
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.timeframe = os.getenv('TIMEFRAME', '1h')
        
        # Initialize Coinbase (no auth needed for public data)
        self.exchange = ccxt.coinbase({
            'enableRateLimit': True,
            'rateLimit': 1000
        })
        
        # Track last signals
        self.last_signals = {}
        
        logger.info("Bot initialized with Coinbase exchange")
    
    def send_telegram_message(self, message: str):
        """Send message via Telegram Bot"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info("Telegram message sent successfully")
            else:
                logger.error(f"Telegram send failed: {response.text}")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
    
    def get_top_coins_by_volume(self, limit: int = 5) -> List[str]:
        """Get top coins by 24h trading volume from Coinbase"""
        try:
            logger.info("Fetching Coinbase tickers...")
            tickers = self.exchange.fetch_tickers()
            
            # Filter USD pairs (Coinbase uses USD, not USDT)
            usd_pairs = {}
            for symbol, ticker in tickers.items():
                if '/USD' in symbol and ticker.get('quoteVolume'):
                    try:
                        volume = float(ticker['quoteVolume'])
                        if volume > 0:
                            usd_pairs[symbol] = ticker
                    except (ValueError, TypeError):
                        continue
            
            # Sort by volume
            sorted_pairs = sorted(
                usd_pairs.items(),
                key=lambda x: float(x[1].get('quoteVolume', 0)),
                reverse=True
            )
            
            top_coins = [pair[0] for pair in sorted_pairs[:limit]]
            logger.info(f"Top {limit} coins by volume (Coinbase): {top_coins}")
            return top_coins
            
        except Exception as e:
            logger.error(f"Error fetching top coins: {e}")
            # Fallback to major pairs
            return ['BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'AVAX/USD']
    
    def calculate_ema(self, data: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average"""
        return data.ewm(span=period, adjust=False).mean()
    
    def fetch_ohlcv_data(self, symbol: str, timeframe: str, limit: int = 100) -> pd.DataFrame:
        """Fetch OHLCV data from Coinbase"""
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    
    def detect_crossover(self, symbol: str, df: pd.DataFrame) -> Dict:
        """Detect EMA crossover signals"""
        if df is None or len(df) < 25:
            return None
        
        # Calculate EMAs
        df['ema_10'] = self.calculate_ema(df['close'], 10)
        df['ema_20'] = self.calculate_ema(df['close'], 20)
        
        # Get last two rows
        current = df.iloc[-1]
        previous = df.iloc[-2]
        
        signal = None
        
        # Bullish crossover
        if previous['ema_10'] <= previous['ema_20'] and current['ema_10'] > current['ema_20']:
            signal = {
                'type': 'BULLISH',
                'symbol': symbol,
                'price': current['close'],
                'ema_10': current['ema_10'],
                'ema_20': current['ema_20'],
                'timestamp': current['timestamp']
            }
        
        # Bearish crossover
        elif previous['ema_10'] >= previous['ema_20'] and current['ema_10'] < current['ema_20']:
            signal = {
                'type': 'BEARISH',
                'symbol': symbol,
                'price': current['close'],
                'ema_10': current['ema_10'],
                'ema_20': current['ema_20'],
                'timestamp': current['timestamp']
            }
        
        return signal
    
    def format_signal_message(self, signal: Dict) -> str:
        """Format signal as Telegram message"""
        emoji = "🟢" if signal['type'] == 'BULLISH' else "🔴"
        
        message = f"""
{emoji} <b>{signal['type']} CROSSOVER DETECTED</b> {emoji}

📊 <b>Symbol:</b> {signal['symbol']}
💰 <b>Price:</b> ${signal['price']:.2f}
📈 <b>EMA(10):</b> {signal['ema_10']:.2f}
📉 <b>EMA(20):</b> {signal['ema_20']:.2f}
⏰ <b>Time:</b> {signal['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}
⏱️ <b>Timeframe:</b> {self.timeframe}
🏦 <b>Exchange:</b> Coinbase
"""
        return message.strip()
    
    def check_signals(self):
        """Main function to check for signals"""
        logger.info(f"Checking signals for timeframe: {self.timeframe}")
        
        # Get top coins
        top_coins = self.get_top_coins_by_volume(5)
        
        signals_found = []
        
        for symbol in top_coins:
            try:
                # Fetch data
                df = self.fetch_ohlcv_data(symbol, self.timeframe, limit=100)
                
                if df is not None:
                    # Detect crossover
                    signal = self.detect_crossover(symbol, df)
                    
                    if signal:
                        # Check if new signal
                        signal_key = f"{symbol}_{signal['type']}"
                        current_time = time.time()
                        
                        if signal_key not in self.last_signals or \
                           (current_time - self.last_signals[signal_key]) > 7200:
                            signals_found.append(signal)
                            self.last_signals[signal_key] = current_time
                            logger.info(f"New signal detected: {signal}")
                
                # Rate limiting
                time.sleep(1.5)
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                continue
        
        # Send signals
        if signals_found:
            for signal in signals_found:
                message = self.format_signal_message(signal)
                self.send_telegram_message(message)
        else:
            logger.info("No new crossover signals detected")
    
    def run(self):
        """Main loop"""
        logger.info("Starting Crypto Signal Bot...")
        self.send_telegram_message(f"🚀 <b>Crypto Signal Bot Started!</b>\n\n🏦 <b>Exchange:</b> Coinbase\n⏱️ <b>Timeframe:</b> {self.timeframe}\n📊 <b>Monitoring:</b> Top 5 coins by volume\n📈 <b>Signal:</b> EMA(10) x EMA(20) crossovers")
        
        while True:
            try:
                # Check signals
                self.check_signals()
                
                # Wait 5 minutes
                logger.info("Waiting 5 minutes before next check...")
                time.sleep(300)
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                self.send_telegram_message("⏹️ Bot stopped")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                self.send_telegram_message(f"⚠️ Error occurred: {str(e)}")
                time.sleep(60)

def run_bot():
    """Run the bot in a separate thread"""
    bot = CryptoSignalBot()
    bot.run()

if __name__ == "__main__":
    # Start bot in background thread
    logger.info("Starting bot in background thread...")
    bot_thread = Thread(target=run_bot, daemon=True)
    bot_thread.start()
    
    # Start Flask web server
    port = int(os.getenv('PORT', 10000))
    logger.info(f"Starting Flask server on port {port}...")
    app.run(host='0.0.0.0', port=port)
