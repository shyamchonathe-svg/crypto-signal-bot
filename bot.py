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
        "timeframe": os.getenv('TIMEFRAME', '1h'),
        "monitoring": "Top 5 coins by volume",
        "signal_type": "EMA(10) x EMA(20) crossovers"
    }

class CryptoSignalBot:
    def __init__(self):
        # Environment variables (set these in your hosting platform)
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.delta_api_key = os.getenv('DELTA_API_KEY')
        self.delta_api_secret = os.getenv('DELTA_API_SECRET')
        self.timeframe = os.getenv('TIMEFRAME', '1h')  # Default 1 hour
        
        # Initialize exchanges - use spot market with rate limiting
        self.binance = ccxt.binance({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot',  # Use spot market, not futures
                'adjustForTimeDifference': True
            },
            'rateLimit': 2000  # Increase delay between requests
        })
        self.delta_exchange = None
        self.setup_delta_exchange()
        
        # Track last signals to avoid duplicates
        self.last_signals = {}
        
    def setup_delta_exchange(self):
        """Setup Delta Exchange with authentication"""
        try:
            if self.delta_api_key and self.delta_api_secret:
                self.delta_exchange = ccxt.delta({
                    'apiKey': self.delta_api_key,
                    'secret': self.delta_api_secret,
                    'enableRateLimit': True
                })
                # Test authentication
                balance = self.delta_exchange.fetch_balance()
                logger.info("Delta Exchange authenticated successfully")
                self.send_telegram_message("✅ Delta Exchange connected successfully!")
            else:
                logger.warning("Delta Exchange credentials not provided")
        except Exception as e:
            logger.error(f"Delta Exchange authentication failed: {e}")
            self.send_telegram_message(f"⚠️ Delta Exchange auth failed: {str(e)}")
            self.delta_exchange = None
    
    def retry_delta_authentication(self):
        """Retry Delta Exchange authentication"""
        logger.info("Retrying Delta Exchange authentication...")
        self.setup_delta_exchange()
    
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
        """Get top coins by 24h trading volume"""
        try:
            tickers = self.binance.fetch_tickers()
            usdt_pairs = {k: v for k, v in tickers.items() if '/USDT' in k and ':USDT' not in k}
            
            # Sort by quote volume (volume in USDT)
            sorted_pairs = sorted(
                usdt_pairs.items(),
                key=lambda x: float(x[1].get('quoteVolume', 0)),
                reverse=True
            )
            
            top_coins = [pair[0] for pair in sorted_pairs[:limit]]
            logger.info(f"Top {limit} coins by volume: {top_coins}")
            return top_coins
        except Exception as e:
            logger.error(f"Error fetching top coins: {e}")
            return ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT']
    
    def calculate_ema(self, data: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average"""
        return data.ewm(span=period, adjust=False).mean()
    
    def fetch_ohlcv_data(self, symbol: str, timeframe: str, limit: int = 100) -> pd.DataFrame:
        """Fetch OHLCV data from exchange"""
        try:
            ohlcv = self.binance.fetch_ohlcv(symbol, timeframe, limit=limit)
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
        
        # Get last two rows for crossover detection
        current = df.iloc[-1]
        previous = df.iloc[-2]
        
        signal = None
        
        # Bullish crossover: EMA10 crosses above EMA20
        if previous['ema_10'] <= previous['ema_20'] and current['ema_10'] > current['ema_20']:
            signal = {
                'type': 'BULLISH',
                'symbol': symbol,
                'price': current['close'],
                'ema_10': current['ema_10'],
                'ema_20': current['ema_20'],
                'timestamp': current['timestamp']
            }
        
        # Bearish crossover: EMA10 crosses below EMA20
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
💰 <b>Price:</b> ${signal['price']:.4f}
📈 <b>EMA(10):</b> {signal['ema_10']:.4f}
📉 <b>EMA(20):</b> {signal['ema_20']:.4f}
⏰ <b>Time:</b> {signal['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}
⏱️ <b>Timeframe:</b> {self.timeframe}
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
                        # Check if this is a new signal (not sent in last 2 hours)
                        signal_key = f"{symbol}_{signal['type']}"
                        current_time = time.time()
                        
                        if signal_key not in self.last_signals or \
                           (current_time - self.last_signals[signal_key]) > 7200:  # 2 hours
                            signals_found.append(signal)
                            self.last_signals[signal_key] = current_time
                            logger.info(f"New signal detected: {signal}")
                
                # Rate limiting
                time.sleep(0.5)
                
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
        self.send_telegram_message(f"🚀 <b>Crypto Signal Bot Started!</b>\n\n⏱️ Timeframe: {self.timeframe}\n📊 Monitoring top 5 coins by volume\n📈 EMA(10) x EMA(20) crossovers")
        
        # Counter for Delta auth retry (every 24 hours)
        auth_retry_counter = 0
        auth_retry_interval = 288  # 24 hours / 5 minutes
        
        while True:
            try:
                # Check signals
                self.check_signals()
                
                # Retry Delta authentication periodically
                auth_retry_counter += 1
                if auth_retry_counter >= auth_retry_interval:
                    self.retry_delta_authentication()
                    auth_retry_counter = 0
                
                # Wait for next check (5 minutes)
                logger.info("Waiting 5 minutes before next check...")
                time.sleep(300)
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                self.send_telegram_message("⏹️ Bot stopped")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                self.send_telegram_message(f"⚠️ Error occurred: {str(e)}")
                time.sleep(60)  # Wait 1 minute before retrying

def run_bot():
    """Run the bot in a separate thread"""
    bot = CryptoSignalBot()
    bot.run()

if __name__ == "__main__":
    # Start bot in background thread
    logger.info("Starting bot in background thread...")
    bot_thread = Thread(target=run_bot, daemon=True)
    bot_thread.start()
    
    # Start Flask web server (keeps Render awake)
    port = int(os.getenv('PORT', 10000))
    logger.info(f"Starting Flask server on port {port}...")
    app.run(host='0.0.0.0', port=port)
