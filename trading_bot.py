
"""
Advanced Trading Bot
-------------------
A robust cryptocurrency trading bot with:
- Market order execution
- Multi-threaded data processing
- Advanced risk management
- Comprehensive error handling
"""

import logging
from logging.handlers import RotatingFileHandler
import os
import time
import numpy as np
from typing import Dict, Optional, List, Tuple, Any, Union
import ccxt
from datetime import datetime, date
from collections import deque
import pandas as pd
import json
import yaml
from decimal import Decimal, ROUND_DOWN
import sys
from pathlib import Path
import requests
import signal
import threading
from concurrent.futures import ThreadPoolExecutor

# System Configuration
SYSTEM_CONFIG = {
    'max_threads': 4,              # Maximum threads for concurrent operations
    'market_data_timeout': 10,     # Timeout for market data requests
    'order_timeout': 30,           # Timeout for order operations
    'balance_cache_time': 60,      # Cache balance data for 60 seconds
    'log_level': 'INFO',          # Default logging level
    'max_log_size': 50*1024*1024, # 50MB max log size
    'log_backups': 10,            # Number of log backups to keep
    'request_timeout': 30,        # Default request timeout
}

# Exchange Configuration
EXCHANGE_CONFIG = {
    'exchange_id': 'binance',
    'api_key': 'YOUR_API_KEY',
    'api_secret': 'YOUR_API_SECRET',
    'test_mode': False,
    'recvWindow': 60000,
    'pairs': {
        'ALGO/USDC': {
            'min_amount': 1,
            'precision': 1,
            'price_precision': 4,
            'min_cost': 5.0,
            'max_position': 1000.0,
            'volatility_threshold': 0.05,  # Max 5% volatility
            'min_volume_score': 0.5,      # Minimum volume score requirement
            'min_signal_confidence': 0.7   # Minimum signal confidence
        },
        'NEO/USDC': {
            'min_amount': 0.1,
            'precision': 3,
            'price_precision': 4,
            'min_cost': 5.0,
            'max_position': 1000.0,
            'volatility_threshold': 0.05,
            'min_volume_score': 0.5,
            'min_signal_confidence': 0.7
        }
    },
    'risk_amount': 15.0,          # Maximum risk per trade in USDC
    'position_loss_limit': 20.0,  # Maximum loss per position in USDC
    'max_trades': 2,              # Maximum concurrent trades
    'trailing_stop': 0.005,       # 0.5% trailing stop
    'take_profit': 0.015,         # 1.5% take profit
    'stop_loss': 0.02,           # 2% stop loss
    'retry_delay': 3,            # Seconds between retries
    'max_retries': 5,            # Maximum retry attempts
    'update_interval': 2,        # Seconds between updates
    'status_interval': 300       # Seconds between status updates
}

# Risk Management Configuration
RISK_CONFIG = {
    'max_daily_loss': 30.0,       # Maximum daily loss in USDC
    'max_position_risk': 0.02,    # Maximum risk per position (2%)
    'volatility_adjustment': True, # Adjust position size based on volatility
    'max_drawdown': 0.1,          # Maximum drawdown before stopping (10%)
    'recovery_threshold': 0.05     # Profit needed to resume after drawdown (5%)
}

# Scanner Configuration
SCANNER_CONFIG = {
    'dex_array': {
        'enabled': True,
        'threshold': 0.166,
        'levels': 2,
        'min_volume': 1.0,
        'max_spread': 0.01
    },
    'counter_spoof': {
        'enabled': True,
        'threshold': 0.4,
        'min_volume': 1.5,
        'lookback_periods': 5
    },
    'volume_filter': {
        'enabled': True,
        'min_24h_volume': 10000,
        'min_trades': 50,
        'volume_trend_periods': 12,
        'min_volume_increase': 1.5
    }
}

# Custom Exceptions
class TradingBotError(Exception):
    """Base exception for trading bot errors"""
    pass

class MarketDataError(TradingBotError):
    """Exception for market data related errors"""
    pass

class OrderError(TradingBotError):
    """Exception for order related errors"""
    pass

class ConfigError(TradingBotError):
    """Exception for configuration related errors"""
    pass

class APIError(TradingBotError):
    """Exception for API related errors"""
    pass

# Basic Utility Functions
def get_external_ip() -> Optional[str]:
    """Get external IP address with retry logic"""
    for _ in range(3):
        try:
            response = requests.get('https://api.ipify.org', 
                                  timeout=SYSTEM_CONFIG['request_timeout'])
            if response.status_code == 200:
                return response.text
        except Exception:
            time.sleep(1)
    return None

def round_amount(amount: float, precision: int) -> float:
    """Round amount to specified precision with validation"""
    try:
        if not isinstance(precision, int):
            raise ValueError("Precision must be an integer")
        if amount < 0:
            raise ValueError("Amount must be positive")
        return float(Decimal(str(amount)).quantize(
            Decimal('1e-' + str(precision)), 
            rounding=ROUND_DOWN
        ))
    except Exception as e:
        raise ValueError(f"Error rounding amount: {str(e)}")

def round_price(price: float, precision: int) -> float:
    """Round price to specified precision with validation"""
    try:
        if not isinstance(precision, int):
            raise ValueError("Precision must be an integer")
        if price < 0:
            raise ValueError("Price must be positive")
        return float(Decimal(str(price)).quantize(
            Decimal('1e-' + str(precision)), 
            rounding=ROUND_DOWN
        ))
    except Exception as e:
        raise ValueError(f"Error rounding price: {str(e)}")


  # Logging Setup
def setup_logger(name: str = 'trading_bot') -> logging.Logger:
    """
    Setup enhanced logging configuration with file rotation and console output
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger instance
    """
    try:
        # Create logs directory with secure permissions
        logs_dir = 'logs'
        os.makedirs(logs_dir, exist_ok=True)
        os.chmod(logs_dir, 0o700)  # Owner read/write/execute only
        
        # Create daily log file
        today = datetime.now().strftime("%Y%m%d")
        log_filename = os.path.join(logs_dir, f'{name}_{today}.log')
        
        # Initialize logger
        logger = logging.getLogger(name)
        logger.setLevel(SYSTEM_CONFIG['log_level'])
        logger.handlers = []  # Clear existing handlers
        
        # Create formatters
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s'
        )
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            log_filename,
            maxBytes=SYSTEM_CONFIG['max_log_size'],
            backupCount=SYSTEM_CONFIG['log_backups']
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)  # File gets all logs
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(SYSTEM_CONFIG['log_level'])
        
        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
        
    except Exception as e:
        raise ConfigError(f"Failed to setup logger: {str(e)}")

# Configuration Management
def load_config(config_path: str = 'config/config.yaml') -> Dict:
    """
    Load and validate configuration from file or create default
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Merged configuration dictionary
    """
    try:
        # Create config directory
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        # Load existing config if present
        if os.path.exists(config_path):
            with open(config_path, 'r') as file:
                loaded_config = yaml.safe_load(file)
                if not loaded_config:
                    loaded_config = {}
        else:
            loaded_config = {}
        
        # Create base config
        config = {
            'system': SYSTEM_CONFIG.copy(),
            'exchange': EXCHANGE_CONFIG.copy(),
            'risk': RISK_CONFIG.copy(),
            'scanner': SCANNER_CONFIG.copy()
        }
        
        # Merge with loaded config
        if loaded_config:
            for section in config:
                if section in loaded_config:
                    config[section].update(loaded_config[section])
        
        # Validate merged config
        validate_config(config)
        
        # Save merged config
        with open(config_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
        
        # Set secure permissions
        os.chmod(config_path, 0o600)  # Owner read/write only
        
        return config
        
    except Exception as e:
        raise ConfigError(f"Error loading configuration: {str(e)}")

def validate_config(config: Dict) -> None:
    """
    Validate configuration settings
    
    Args:
        config: Configuration dictionary to validate
        
    Raises:
        ConfigError: If validation fails
    """
    try:
        required_sections = ['system', 'exchange', 'risk', 'scanner']
        
        # Check required sections
        for section in required_sections:
            if section not in config:
                raise ConfigError(f"Missing configuration section: {section}")
        
        # Validate system config
        sys_config = config['system']
        if sys_config.get('max_threads', 0) <= 0:
            raise ConfigError("Invalid max_threads value")
        if sys_config.get('market_data_timeout', 0) <= 0:
            raise ConfigError("Invalid market_data_timeout value")
        
        # Validate exchange config
        ex_config = config['exchange']
        if not ex_config.get('api_key'):
            raise ConfigError("Missing API key")
        if not ex_config.get('api_secret'):
            raise ConfigError("Missing API secret")
        if not ex_config.get('pairs'):
            raise ConfigError("No trading pairs specified")
        
        # Validate trading pairs
        for symbol, pair_config in ex_config['pairs'].items():
            required_pair_fields = [
                'min_amount', 'precision', 'price_precision',
                'min_cost', 'max_position', 'volatility_threshold',
                'min_volume_score', 'min_signal_confidence'
            ]
            
            for field in required_pair_fields:
                if field not in pair_config:
                    raise ConfigError(f"Missing {field} for pair {symbol}")
                if pair_config[field] <= 0:
                    raise ConfigError(f"Invalid {field} value for pair {symbol}")
        
        # Validate risk config
        risk_config = config['risk']
        if risk_config.get('max_daily_loss', 0) <= 0:
            raise ConfigError("Invalid max_daily_loss value")
        if not 0 < risk_config.get('max_position_risk', 0) < 1:
            raise ConfigError("Invalid max_position_risk value")
        if not 0 < risk_config.get('max_drawdown', 0) < 1:
            raise ConfigError("Invalid max_drawdown value")
        
        # Validate scanner config
        scan_config = config['scanner']
        if not scan_config.get('dex_array', {}).get('enabled'):
            raise ConfigError("DEX array scanner must be enabled")
        if scan_config['volume_filter'].get('min_24h_volume', 0) <= 0:
            raise ConfigError("Invalid min_24h_volume value")
        
    except KeyError as e:
        raise ConfigError(f"Missing required configuration field: {str(e)}")
    except Exception as e:
        raise ConfigError(f"Configuration validation failed: {str(e)}")

def create_required_directories() -> None:
    """Create required directories with secure permissions"""
    try:
        directories = ['logs', 'data', 'config', 'reports']
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            os.chmod(directory, 0o700)  # Owner read/write/execute only
            
    except Exception as e:
        raise ConfigError(f"Failed to create directories: {str(e)}")

def setup_error_reporting() -> None:
    """Setup enhanced error reporting and monitoring"""
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        logger = logging.getLogger('trading_bot')
        logger.critical(
            "Uncaught exception",
            exc_info=(exc_type, exc_value, exc_traceback)
        )
        
        # Add enhanced error handling for critical errors
        if issubclass(exc_type, (APIError, OrderError)):
            alert_message = f"Critical error: {str(exc_value)}"
            logger.critical(alert_message)
    
    sys.excepthook = handle_exception

# Initialize Configuration
try:
    # Create required directories
    create_required_directories()
    
    # Setup error reporting
    setup_error_reporting()
    
    # Setup logging
    logger = setup_logger()
    
    # Load configuration
    config = load_config()
    
    # Update global configs
    SYSTEM_CONFIG.update(config['system'])
    EXCHANGE_CONFIG.update(config['exchange'])
    RISK_CONFIG.update(config['risk'])
    SCANNER_CONFIG.update(config['scanner'])
    
except Exception as e:
    print(f"Fatal error during initialization: {str(e)}")
    sys.exit(1)


class MarketDataManager:
    """
    Enhanced Market Data Manager with thread-safe operations and caching
    
    Handles:
    - Market data fetching and caching
    - Volume analysis
    - Signal detection
    - Multi-threaded updates
    """
    
    def __init__(self, exchange, pairs: Dict, logger: logging.Logger):
        """
        Initialize Market Data Manager
        
        Args:
            exchange: CCXT exchange instance
            pairs: Dictionary of trading pairs and their configurations
            logger: Logger instance
        """
        self.exchange = exchange
        self.pairs = pairs
        self.logger = logger
        
        # Initialize components
        self._initialize_caches()
        self._initialize_threading()
        self._initialize_market_data()
        
        self.logger.info("Market Data Manager initialized successfully")
    
    def _initialize_caches(self):
        """Initialize data caches with TTL settings"""
        # Cache containers
        self.ticker_cache = {}
        self.orderbook_cache = {}
        self.trades_cache = {}
        self.volume_cache = {}
        
        # Cache TTL in seconds
        self.ticker_ttl = 2
        self.orderbook_ttl = 1
        self.trades_ttl = 5
        self.volume_ttl = 30
    
    def _initialize_threading(self):
        """Initialize thread pool and locks"""
        try:
            # Create thread pool for concurrent operations
            self.thread_pool = ThreadPoolExecutor(
                max_workers=SYSTEM_CONFIG['max_threads'],
                thread_name_prefix='market_data_'
            )
            
            # Create locks for thread safety
            self.cache_lock = threading.Lock()
            self.update_lock = threading.Lock()
            
        except Exception as e:
            self.logger.error(f"Error initializing threading: {str(e)}")
            raise ConfigError(f"Threading initialization failed: {str(e)}")
    
    def _initialize_market_data(self):
        """Initialize market data containers for each pair"""
        self.market_data = {
            symbol: {
                'ticker': None,
                'orderbook': None,
                'trades': deque(maxlen=100),
                'volume': 0.0,
                'last_update': 0,
                'error_count': 0,
                'signals': {
                    'dex_array': {'signal': False, 'confidence': 0.0},
                    'counter_spoof': {'signal': False, 'confidence': 0.0}
                }
            }
            for symbol in self.pairs
        }
    
    def update_market_data(self) -> bool:
        """
        Update market data concurrently for all pairs
        
        Returns:
            bool: True if all updates successful, False otherwise
        """
        if not self.update_lock.acquire(blocking=False):
            self.logger.warning("Update already in progress, skipping...")
            return False
        
        try:
            success_count = 0
            failed_pairs = []
            
            # Create update tasks for each pair
            update_futures = {
                self.thread_pool.submit(self._update_pair_data, symbol): symbol
                for symbol in self.pairs
            }
            
            # Wait for all updates to complete with timeout
            for future in update_futures:
                try:
                    symbol = update_futures[future]
                    result = future.result(timeout=SYSTEM_CONFIG['market_data_timeout'])
                    
                    if result:
                        success_count += 1
                        self.market_data[symbol]['error_count'] = 0
                    else:
                        failed_pairs.append(symbol)
                        self.market_data[symbol]['error_count'] += 1
                        
                except Exception as e:
                    symbol = update_futures[future]
                    self.logger.error(f"Error updating {symbol}: {str(e)}")
                    failed_pairs.append(symbol)
                    self.market_data[symbol]['error_count'] += 1
            
            if failed_pairs:
                self.logger.warning(f"Failed to update data for pairs: {failed_pairs}")
            
            update_success = success_count == len(self.pairs)
            if not update_success:
                self.logger.warning(
                    f"Market data update partially failed. "
                    f"Success: {success_count}/{len(self.pairs)}"
                )
            
            return update_success
            
        except Exception as e:
            self.logger.error(f"Critical error in market data update: {str(e)}")
            return False
            
        finally:
            self.update_lock.release()
    
    def _update_pair_data(self, symbol: str) -> bool:
        """
        Update market data for a single pair
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            bool: True if update successful, False otherwise
        """
        try:
            current_time = time.time()
            updates_successful = True
            
            # Update ticker
            with self.cache_lock:
                if (current_time - self.ticker_cache.get(symbol, {}).get('timestamp', 0) 
                    > self.ticker_ttl):
                    try:
                        ticker = self.exchange.fetch_ticker(symbol)
                        self.ticker_cache[symbol] = {
                            'data': ticker,
                            'timestamp': current_time
                        }
                        self.market_data[symbol]['ticker'] = ticker
                    except Exception as e:
                        self.logger.error(f"Error fetching ticker for {symbol}: {str(e)}")
                        updates_successful = False
            
            # Update orderbook
            with self.cache_lock:
                if (current_time - self.orderbook_cache.get(symbol, {}).get('timestamp', 0) 
                    > self.orderbook_ttl):
                    try:
                        orderbook = self.exchange.fetch_order_book(symbol, limit=20)
                        self.orderbook_cache[symbol] = {
                            'data': orderbook,
                            'timestamp': current_time
                        }
                        self.market_data[symbol]['orderbook'] = orderbook
                    except Exception as e:
                        self.logger.error(f"Error fetching orderbook for {symbol}: {str(e)}")
                        updates_successful = False
            
            # Update trades
            with self.cache_lock:
                if (current_time - self.trades_cache.get(symbol, {}).get('timestamp', 0) 
                    > self.trades_ttl):
                    try:
                        trades = self.exchange.fetch_trades(symbol, limit=100)
                        self.trades_cache[symbol] = {
                            'data': trades,
                            'timestamp': current_time
                        }
                        self.market_data[symbol]['trades'].extend(trades)
                    except Exception as e:
                        self.logger.error(f"Error fetching trades for {symbol}: {str(e)}")
                        updates_successful = False
            
            # Update signal analysis if data is available
            if updates_successful:
                try:
                    dex_signal, dex_confidence = self.analyze_dex_array(symbol)
                    spoof_signal, spoof_confidence = self.detect_counter_spoofing(symbol)
                    
                    self.market_data[symbol]['signals']['dex_array'] = {
                        'signal': dex_signal,
                        'confidence': dex_confidence
                    }
                    self.market_data[symbol]['signals']['counter_spoof'] = {
                        'signal': spoof_signal,
                        'confidence': spoof_confidence
                    }
                except Exception as e:
                    self.logger.error(f"Error updating signals for {symbol}: {str(e)}")
                    updates_successful = False
            
            self.market_data[symbol]['last_update'] = current_time
            return updates_successful
            
        except Exception as e:
            self.logger.error(f"Critical error updating data for {symbol}: {str(e)}")
            return False
    
    def get_ticker(self, symbol: str) -> Optional[Dict]:
        """Get cached ticker data with validation"""
        try:
            with self.cache_lock:
                cached = self.ticker_cache.get(symbol, {}).get('data')
                if cached and time.time() - self.ticker_cache[symbol]['timestamp'] <= self.ticker_ttl:
                    return cached
                
                try:
                    ticker = self.exchange.fetch_ticker(symbol)
                    self.ticker_cache[symbol] = {
                        'data': ticker,
                        'timestamp': time.time()
                    }
                    return ticker
                except Exception as e:
                    self.logger.error(f"Error fetching fresh ticker for {symbol}: {str(e)}")
                    return cached  # Return cached data if fetch fails
        except Exception as e:
            self.logger.error(f"Error in get_ticker for {symbol}: {str(e)}")
            return None
    
    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """Get cached orderbook data with validation"""
        try:
            with self.cache_lock:
                cached = self.orderbook_cache.get(symbol, {}).get('data')
                if cached and time.time() - self.orderbook_cache[symbol]['timestamp'] <= self.orderbook_ttl:
                    return cached
                
                try:
                    orderbook = self.exchange.fetch_order_book(symbol, limit=20)
                    self.orderbook_cache[symbol] = {
                        'data': orderbook,
                        'timestamp': time.time()
                    }
                    return orderbook
                except Exception as e:
                    self.logger.error(f"Error fetching fresh orderbook for {symbol}: {str(e)}")
                    return cached
        except Exception as e:
            self.logger.error(f"Error in get_orderbook for {symbol}: {str(e)}")
            return None

    # (Market Analysis Methods continue in next message...)


    def calculate_spread(self, symbol: str) -> Optional[float]:
        """
        Calculate bid-ask spread with improved validation
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            float: Spread as a percentage, or None if invalid
        """
        try:
            orderbook = self.get_orderbook(symbol)
            if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
                return None
            
            best_bid = float(orderbook['bids'][0][0])
            best_ask = float(orderbook['asks'][0][0])
            
            if best_bid <= 0 or best_ask <= 0:
                self.logger.warning(f"Invalid bid/ask prices for {symbol}")
                return None
            
            spread = (best_ask - best_bid) / best_bid
            
            # Validate spread is reasonable
            if spread < 0 or spread > 1:
                self.logger.warning(f"Abnormal spread ({spread}) detected for {symbol}")
                return None
            
            return spread
            
        except Exception as e:
            self.logger.error(f"Error calculating spread for {symbol}: {str(e)}")
            return None
    
    def analyze_volume(self, symbol: str) -> Tuple[bool, float]:
        """
        Analyze trading volume with enhanced criteria
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Tuple[bool, float]: (volume_ok, volume_score)
        """
        try:
            current_time = time.time()
            
            # Check cache first
            with self.cache_lock:
                if symbol in self.volume_cache:
                    cache_data = self.volume_cache[symbol]
                    if current_time - cache_data['timestamp'] <= self.volume_ttl:
                        return cache_data['result']
            
            # Get required data
            ticker = self.get_ticker(symbol)
            if not ticker or 'quoteVolume' not in ticker:
                return False, 0.0
            
            trades = list(self.market_data[symbol]['trades'])
            if len(trades) < SCANNER_CONFIG['volume_filter']['min_trades']:
                return False, 0.0
            
            # Calculate core volume metrics
            volume_24h = float(ticker['quoteVolume'])
            min_volume = SCANNER_CONFIG['volume_filter']['min_24h_volume']
            
            # Calculate recent volume trend
            recent_trades = trades[-50:]  # Last 50 trades
            recent_volume = sum(float(trade['amount']) for trade in recent_trades)
            
            # Calculate volume consistency
            volume_std = np.std([float(trade['amount']) for trade in recent_trades])
            volume_mean = np.mean([float(trade['amount']) for trade in recent_trades])
            volume_consistency = 1 - min(1, volume_std / (volume_mean + 1e-8))
            
            # Calculate volume trend
            if len(trades) >= 100:
                old_trades = trades[-100:-50]  # Previous 50 trades
                old_volume = sum(float(trade['amount']) for trade in old_trades)
                if old_volume > 0:
                    volume_trend = recent_volume / old_volume
                else:
                    volume_trend = 0
            else:
                volume_trend = 1
            
            # Calculate final volume score (0.0 to 1.0)
            base_score = min(1.0, (volume_24h / min_volume))
            trend_score = min(1.0, volume_trend / SCANNER_CONFIG['volume_filter']['min_volume_increase'])
            consistency_score = volume_consistency
            
            volume_score = (base_score * 0.4 + trend_score * 0.4 + consistency_score * 0.2)
            
            # Cache result
            with self.cache_lock:
                self.volume_cache[symbol] = {
                    'result': (volume_score >= SCANNER_CONFIG['volume_filter']['min_volume'], volume_score),
                    'timestamp': current_time
                }
            
            return volume_score >= SCANNER_CONFIG['volume_filter']['min_volume'], volume_score
            
        except Exception as e:
            self.logger.error(f"Error analyzing volume for {symbol}: {str(e)}")
            return False, 0.0
    
    def analyze_dex_array(self, symbol: str) -> Tuple[bool, float]:
        """
        Analyze price action using DEX array with improved signal detection
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Tuple[bool, float]: (signal, confidence)
        """
        try:
            trades = list(self.market_data[symbol]['trades'])
            if len(trades) < 20:
                return False, 0.0
            
            # Convert trades to DataFrame with error handling
            try:
                df = pd.DataFrame([{
                    'price': float(trade['price']),
                    'amount': float(trade['amount']),
                    'timestamp': trade['timestamp'],
                    'side': trade.get('side', '')
                } for trade in trades])
            except Exception as e:
                self.logger.error(f"Error creating DataFrame for {symbol}: {str(e)}")
                return False, 0.0
            
            # Calculate price metrics
            df['vwap'] = (df['price'] * df['amount']).cumsum() / df['amount'].cumsum()
            df['price_std'] = df['price'].rolling(20).std()
            df['price_mean'] = df['price'].rolling(20).mean()
            df['z_score'] = (df['price'] - df['price_mean']) / df['price_std']
            
            # Calculate volume-weighted metrics
            df['vol_price'] = df['price'] * df['amount']
            df['weighted_price'] = df['vol_price'].rolling(20).sum() / df['amount'].rolling(20).sum()
            
            # Detect price trends
            df['price_change'] = df['price'].pct_change()
            df['trend'] = df['price_change'].rolling(10).mean()
            
            # Get latest metrics
            latest_price = df['price'].iloc[-1]
            latest_vwap = df['vwap'].iloc[-1]
            latest_std = df['price_std'].iloc[-1]
            latest_zscore = df['z_score'].iloc[-1]
            latest_trend = df['trend'].iloc[-1]
            
            # Calculate signal strength components
            price_deviation = abs(latest_price - latest_vwap) / latest_std
            trend_strength = abs(latest_trend)
            zscore_strength = abs(latest_zscore)
            
            # Combine signal components
            signal_strength = (
                price_deviation * 0.4 +
                trend_strength * 0.4 +
                zscore_strength * 0.2
            )
            
            # Normalize signal strength
            signal_strength = min(1.0, signal_strength)
            
            # Generate signal
            signal = signal_strength > SCANNER_CONFIG['dex_array']['threshold']
            
            return signal, signal_strength
            
        except Exception as e:
            self.logger.error(f"Error analyzing DEX array for {symbol}: {str(e)}")
            return False, 0.0
    
    def detect_counter_spoofing(self, symbol: str) -> Tuple[bool, float]:
        """
        Detect counter spoofing with improved accuracy
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Tuple[bool, float]: (signal, confidence)
        """
        try:
            orderbook = self.get_orderbook(symbol)
            if not orderbook:
                return False, 0.0
            
            # Get order book levels
            bids = orderbook['bids'][:5]
            asks = orderbook['asks'][:5]
            
            if not bids or not asks:
                return False, 0.0
            
            try:
                # Calculate volume-weighted prices
                bid_volume = sum(float(bid[1]) for bid in bids)
                ask_volume = sum(float(ask[1]) for ask in asks)
                
                bid_vwap = sum(float(bid[0]) * float(bid[1]) for bid in bids) / bid_volume
                ask_vwap = sum(float(ask[0]) * float(ask[1]) for ask in asks) / ask_volume
                
                # Calculate imbalance metrics
                volume_imbalance = abs(bid_volume - ask_volume) / (bid_volume + ask_volume)
                price_imbalance = abs(bid_vwap - ask_vwap) / ((bid_vwap + ask_vwap) / 2)
                
                # Analyze order distribution
                bid_sizes = [float(bid[1]) for bid in bids]
                ask_sizes = [float(ask[1]) for ask in asks]
                
                bid_size_std = np.std(bid_sizes) / (np.mean(bid_sizes) + 1e-8)
                ask_size_std = np.std(ask_sizes) / (np.mean(ask_sizes) + 1e-8)
                
                size_irregularity = (bid_size_std + ask_size_std) / 2
                
                # Calculate final confidence score
                confidence = (
                    volume_imbalance * 0.4 +
                    price_imbalance * 0.4 +
                    size_irregularity * 0.2
                )
                
                # Normalize confidence
                confidence = min(1.0, confidence)
                
                # Generate signal
                signal = confidence > SCANNER_CONFIG['counter_spoof']['threshold']
                
                return signal, confidence
                
            except Exception as e:
                self.logger.error(f"Error in counter spoofing calculations for {symbol}: {str(e)}")
                return False, 0.0
            
        except Exception as e:
            self.logger.error(f"Error detecting counter spoofing for {symbol}: {str(e)}")
            return False, 0.0
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.thread_pool.shutdown(wait=True)
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
class TradeManager:
    def __init__(self, exchange, pairs: Dict, risk_amount: float, logger: logging.Logger):
        """Initialize Trade Manager"""
        self.exchange = exchange
        self.pairs = pairs
        self.risk_amount = risk_amount
        self.logger = logger
        
        # Initialize locks
        self._initialize_locks()
        
        # Initialize tracking systems
        self._initialize_tracking()
        self.peak_balance = 0.0
        
        # Initialize risk metrics
        self._initialize_risk_metrics()
        
        self.logger.info("Trade Manager initialized successfully")


    

    def _initialize_locks(self):
        """Initialize threading locks"""
        self.balance_lock = threading.Lock()
        self.order_lock = threading.Lock()
        self.metrics_lock = threading.Lock()
        self.position_lock = threading.Lock()
    
    def _initialize_tracking(self):
        """Initialize trade tracking systems"""
        self.active_orders = {}
        self.balance_cache = {'timestamp': 0, 'data': None}
        self.trade_history = deque(maxlen=1000)
        
        self.performance_metrics = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_profit': 0.0,
            'total_loss': 0.0,
            'largest_win': 0.0,
            'largest_loss': 0.0,
            'average_win': 0.0,
            'average_loss': 0.0,
            'win_rate': 0.0,
            'profit_factor': 0.0,
            'avg_trade_duration': 0.0,
            'consecutive_losses': 0,
            'max_consecutive_losses': 0
        }
    
    def _initialize_risk_metrics(self):
        """Initialize risk management metrics"""
        try:
            self.daily_loss = 0.0
            self.max_drawdown = 0.0
            self.initial_balance = self._get_account_balance()
            self.peak_balance = self.initial_balance
            
            if self.initial_balance <= 0:
                raise ValueError("Failed to get initial balance")
                
            self.logger.info(f"Initial balance: {self.initial_balance:.2f} USDC")
            
        except Exception as e:
            self.logger.error(f"Error initializing risk metrics: {str(e)}")
            raise
    
    def _get_account_balance(self) -> float:
        """Get account balance with caching"""
        try:
            current_time = time.time()
            
            with self.balance_lock:
                if (current_time - self.balance_cache['timestamp'] 
                    <= SYSTEM_CONFIG['balance_cache_time']):
                    return self.balance_cache['data']
                
                balance = self.exchange.fetch_balance()
                usdc_balance = float(balance.get('USDC', {}).get('free', 0))
                
                # Update cache
                self.balance_cache = {
                    'timestamp': current_time,
                    'data': usdc_balance
                }
                
                # Update peak balance and drawdown
                if usdc_balance > self.peak_balance:
                    self.peak_balance = usdc_balance
                else:
                    current_drawdown = (self.peak_balance - usdc_balance) / self.peak_balance
                    self.max_drawdown = max(self.max_drawdown, current_drawdown)
                
                return usdc_balance
            
        except Exception as e:
            self.logger.error(f"Error fetching balance: {str(e)}")
            return 0.0
    
    def place_order(self, symbol: str, side: str, amount: float, price: float, 
                   order_type: str = 'market') -> Optional[Dict]:
        """Place order with enhanced error handling"""
        with self.order_lock:
            try:
                # Validate inputs
                if not all([symbol, side, amount]):
                    raise ValueError("Missing required order parameters")
                
                # Get pair info
                pair_info = self.pairs.get(symbol)
                if not pair_info:
                    raise ValueError(f"Invalid symbol: {symbol}")
                
                # Round amount
                amount = round_amount(amount, pair_info['precision'])
                
                # Get current price
                ticker = self.exchange.fetch_ticker(symbol)
                current_price = float(ticker['ask']) if side == 'buy' else float(ticker['bid'])
                
                # Calculate order cost with slippage buffer
                slippage_buffer = 1.01  # 1% buffer
                order_cost = amount * current_price * slippage_buffer
                
                # Check balance
                available_balance = self._get_account_balance()
                if order_cost > available_balance:
                    self.logger.warning(
                        f"Insufficient balance for {symbol} {side} order. "
                        f"Need: {order_cost:.2f}, Have: {available_balance:.2f}"
                    )
                    return None
                
                # Check risk limits
                if not self._check_risk_limits(order_cost):
                    return None
                
                # Prepare order
                order_params = {
                    'symbol': symbol,
                    'type': 'market',
                    'side': side,
                    'amount': amount,
                    'params': {
                        'timeInForce': 'IOC'
                    }
                }
                
                self.logger.info(
                    f"Placing {side} market order: {amount} {symbol} "
                    f"@ ~{current_price:.8f}"
                )
                
                # Place order with retries
                for attempt in range(EXCHANGE_CONFIG['max_retries']):
                    try:
                        order = self.exchange.create_order(**order_params)
                        
                        # Verify execution
                        filled_amount = float(order.get('filled', 0))
                        if filled_amount == 0:
                            raise OrderError("Order was placed but not filled")
                        
                        average_price = float(order.get('average', current_price))
                        
                        self.logger.info(
                            f"Successfully executed {side} market order for "
                            f"{filled_amount} {symbol} @ avg price {average_price:.8f}"
                        )
                        
                        # Store order details
                        order_info = {
                            'order': order,
                            'symbol': symbol,
                            'side': side,
                            'amount': filled_amount,
                            'price': average_price,
                            'timestamp': time.time(),
                            'status': 'filled'
                        }
                        
                        with self.metrics_lock:
                            self.active_orders[order['id']] = order_info
                            self.trade_history.append(order_info)
                            self._update_performance_metrics(order_info)
                        
                        return order
                        
                    except Exception as e:
                        if attempt == EXCHANGE_CONFIG['max_retries'] - 1:
                            raise OrderError(
                                f"Order placement failed after {attempt + 1} attempts: {str(e)}"
                            )
                        
                        self.logger.warning(
                            f"Order attempt {attempt + 1} failed: {str(e)}, retrying..."
                        )
                        time.sleep(EXCHANGE_CONFIG['retry_delay'])
                        
            except Exception as e:
                self.logger.error(f"Error placing order: {str(e)}")
                return None
    
    def _check_risk_limits(self, order_cost: float) -> bool:
        """Check if order meets risk management criteria"""
        try:
            # Check daily loss limit
            max_position_risk = order_cost * RISK_CONFIG['max_position_risk']
            if self.daily_loss + max_position_risk > RISK_CONFIG['max_daily_loss']:
                self.logger.warning(
                    f"Daily loss limit would be exceeded. "
                    f"Current loss: {self.daily_loss:.2f}, "
                    f"Max allowed: {RISK_CONFIG['max_daily_loss']:.2f}"
                )
                return False
            
            # Check drawdown limit
            if self.max_drawdown >= RISK_CONFIG['max_drawdown']:
                self.logger.warning(
                    f"Maximum drawdown limit reached: {self.max_drawdown:.1%}"
                )
                return False
            
            # Check consecutive losses
            if (self.performance_metrics['consecutive_losses'] >= 
                RISK_CONFIG.get('max_consecutive_losses', 5)):
                self.logger.warning(
                    f"Maximum consecutive losses reached: "
                    f"{self.performance_metrics['consecutive_losses']}"
                )
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking risk limits: {str(e)}")
            return False
    
    def close_position(self, symbol: str, position: Dict, market_close: bool = False) -> bool:
        """Close position with market orders"""
        with self.position_lock:
            try:
                if not position:
                    return False
                
                close_side = 'sell' if position['direction'] == 'buy' else 'buy'
                
                # Place market order
                order = self.place_order(
                    symbol=symbol,
                    side=close_side,
                    amount=position['position_size'],
                    price=0,
                    order_type='market'
                )
                
                if order:
                    # Calculate P&L
                    entry_price = position['entry_price']
                    exit_price = float(order.get('average', 0))
                    position_size = position['position_size']
                    
                    pnl = (exit_price - entry_price) * position_size if close_side == 'sell' \
                          else (entry_price - exit_price) * position_size
                    
                    # Update metrics
                    with self.metrics_lock:
                        self._update_trade_metrics({
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'size': position_size,
                            'pnl': pnl,
                            'duration': time.time() - position['entry_time']
                        })
                    
                    self.logger.info(
                        f"Closed position {symbol} with P&L: {pnl:.2f} USDC "
                        f"(Entry: {entry_price:.8f}, Exit: {exit_price:.8f})"
                    )
                    
                    return True
                
                return False
                
            except Exception as e:
                self.logger.error(f"Error closing position: {str(e)}")
                return False

    # (Position sizing and metrics update methods continue in next message...)
    def calculate_position_size(self, symbol: str, entry_price: float, stop_loss: float) -> Optional[float]:
        """
        Calculate position size with dynamic risk adjustment
        
        Args:
            symbol: Trading pair symbol
            entry_price: Planned entry price
            stop_loss: Stop loss price
            
        Returns:
            float: Position size or None if invalid
        """
        try:
            pair_info = self.pairs.get(symbol)
            if not pair_info:
                return None
            
            # Calculate risk per unit
            risk_per_unit = abs(entry_price - stop_loss)
            if risk_per_unit <= 0:
                self.logger.warning(f"Invalid risk per unit for {symbol}")
                return None
            
            # Get current market conditions
            ticker = self.exchange.fetch_ticker(symbol)
            
            # Calculate volatility
            high = float(ticker['high'])
            low = float(ticker['low'])
            volatility = (high - low) / low
            
            # Apply dynamic risk adjustments
            risk_multipliers = []
            
            # Volatility adjustment
            if RISK_CONFIG['volatility_adjustment']:
                volatility_multiplier = max(0.5, 1 - volatility * 10)
                risk_multipliers.append(volatility_multiplier)
            
            # Win rate adjustment
            if self.performance_metrics['total_trades'] > 0:
                win_rate = self.performance_metrics['win_rate']
                win_rate_multiplier = min(1.2, max(0.5, win_rate * 2))
                risk_multipliers.append(win_rate_multiplier)
            
            # Drawdown adjustment
            if self.max_drawdown > 0:
                drawdown_multiplier = max(0.5, 1 - self.max_drawdown * 5)
                risk_multipliers.append(drawdown_multiplier)
            
            # Apply consolidated risk adjustment
            if risk_multipliers:
                adjustment = np.mean(risk_multipliers)
                adjusted_risk = self.risk_amount * adjustment
            else:
                adjusted_risk = self.risk_amount
            
            # Calculate initial position size
            position_size = adjusted_risk / risk_per_unit
            
            # Apply position limits
            max_position = pair_info['max_position']
            position_size = min(position_size, max_position)
            
            # Check available balance
            balance_limit = self._get_account_balance() * 0.95  # 95% of available balance
            position_cost = position_size * entry_price
            if position_cost > balance_limit:
                position_size = (balance_limit / entry_price) * 0.95  # Additional safety margin
            
            # Check minimum amount
            min_amount = pair_info['min_amount']
            if position_size < min_amount:
                self.logger.warning(
                    f"Calculated position {position_size} below minimum {min_amount} "
                    f"for {symbol}"
                )
                return None
            
            # Round to appropriate precision
            position_size = round_amount(position_size, pair_info['precision'])
            
            self.logger.info(
                f"Calculated position size for {symbol}: {position_size} "
                f"(Risk: {adjusted_risk:.2f} USDC, "
                f"Volatility: {volatility:.1%})"
            )
            
            return position_size
            
        except Exception as e:
            self.logger.error(f"Error calculating position size: {str(e)}")
            return None
    
    def _update_performance_metrics(self, order_info: Dict):
        """
        Update performance metrics with enhanced tracking
        
        Args:
            order_info: Completed order information
        """
        try:
            with self.metrics_lock:
                self.performance_metrics['total_trades'] += 1
                
                # Calculate trade P&L
                side = order_info['side']
                filled_amount = float(order_info['amount'])
                avg_price = float(order_info['price'])
                order_price = float(order_info['order']['price'])
                
                pnl = (avg_price - order_price) * filled_amount if side == 'sell' \
                      else (order_price - avg_price) * filled_amount
                
                # Update win/loss metrics
                if pnl > 0:
                    self.performance_metrics['winning_trades'] += 1
                    self.performance_metrics['total_profit'] += pnl
                    self.performance_metrics['largest_win'] = max(
                        self.performance_metrics['largest_win'], pnl
                    )
                    self.performance_metrics['consecutive_losses'] = 0
                else:
                    self.performance_metrics['losing_trades'] += 1
                    self.performance_metrics['total_loss'] += abs(pnl)
                    self.performance_metrics['largest_loss'] = min(
                        self.performance_metrics['largest_loss'], pnl
                    )
                    self.performance_metrics['consecutive_losses'] += 1
                    self.performance_metrics['max_consecutive_losses'] = max(
                        self.performance_metrics['max_consecutive_losses'],
                        self.performance_metrics['consecutive_losses']
                    )
                
                # Update averages
                total_trades = self.performance_metrics['total_trades']
                winning_trades = self.performance_metrics['winning_trades']
                losing_trades = self.performance_metrics['losing_trades']
                
                if total_trades > 0:
                    self.performance_metrics['win_rate'] = winning_trades / total_trades
                    
                if winning_trades > 0:
                    self.performance_metrics['average_win'] = \
                        self.performance_metrics['total_profit'] / winning_trades
                
                if losing_trades > 0:
                    self.performance_metrics['average_loss'] = \
                        self.performance_metrics['total_loss'] / losing_trades
                
                # Calculate profit factor
                total_loss = self.performance_metrics['total_loss']
                if total_loss > 0:
                    self.performance_metrics['profit_factor'] = \
                        self.performance_metrics['total_profit'] / total_loss
                
                # Update drawdown tracking
                current_balance = self._get_account_balance()
                drawdown = (self.peak_balance - current_balance) / self.peak_balance
                self.max_drawdown = max(self.max_drawdown, drawdown)
                
                # Update daily loss if trade was losing
                if pnl < 0:
                    self.daily_loss += abs(pnl)
            
        except Exception as e:
            self.logger.error(f"Error updating performance metrics: {str(e)}")
    
    def _update_trade_metrics(self, trade_info: Dict):
        """
        Update trade-specific metrics
        
        Args:
            trade_info: Trade completion information
        """
        try:
            with self.metrics_lock:
                # Update trade duration tracking
                current_trades = self.performance_metrics['total_trades']
                if current_trades > 0:
                    current_avg_duration = self.performance_metrics['avg_trade_duration']
                    new_duration = trade_info['duration']
                    
                    self.performance_metrics['avg_trade_duration'] = (
                        (current_avg_duration * (current_trades - 1) + new_duration)
                        / current_trades
                    )
                
                # Log trade completion
                self.logger.info(
                    f"Trade completed - "
                    f"PnL: {trade_info['pnl']:.2f} USDC, "
                    f"Duration: {trade_info['duration']:.1f}s, "
                    f"Entry: {trade_info['entry_price']:.8f}, "
                    f"Exit: {trade_info['exit_price']:.8f}"
                )
                
        except Exception as e:
            self.logger.error(f"Error updating trade metrics: {str(e)}")
    
    def get_performance_summary(self) -> Dict:
        """
        Get comprehensive performance summary
        
        Returns:
            Dict: Performance metrics and statistics
        """
        try:
            with self.metrics_lock:
                current_balance = self._get_account_balance()
                total_pnl = current_balance - self.initial_balance
                
                return {
                    'current_balance': current_balance,
                    'total_pnl': total_pnl,
                    'total_pnl_percent': (total_pnl / self.initial_balance * 100) if self.initial_balance > 0 else 0,
                    'max_drawdown': self.max_drawdown * 100,
                    'win_rate': self.performance_metrics['win_rate'] * 100,
                    'profit_factor': self.performance_metrics['profit_factor'],
                    'total_trades': self.performance_metrics['total_trades'],
                    'winning_trades': self.performance_metrics['winning_trades'],
                    'losing_trades': self.performance_metrics['losing_trades'],
                    'average_win': self.performance_metrics['average_win'],
                    'average_loss': self.performance_metrics['average_loss'],
                    'largest_win': self.performance_metrics['largest_win'],
                    'largest_loss': self.performance_metrics['largest_loss'],
                    'consecutive_losses': self.performance_metrics['consecutive_losses'],
                    'max_consecutive_losses': self.performance_metrics['max_consecutive_losses'],
                    'average_trade_duration': self.performance_metrics['avg_trade_duration']
                }
                
        except Exception as e:
            self.logger.error(f"Error getting performance summary: {str(e)}")
            return {}
    
    def cleanup(self):
        """Cleanup resources and close positions"""
        try:
            self.logger.info("Cleaning up Trade Manager resources...")
            
            # Save final performance metrics
            summary = self.get_performance_summary()
            self.logger.info(f"Final performance summary: {summary}")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

class TradingBot:
    def __init__(self, exchange_id: str, api_key: str, api_secret: str, test_mode: bool = False):
        """Initialize Trading Bot"""
        self.logger = setup_logger('trading_bot')
        self.logger.info("Initializing Trading Bot...")
        
        try:
            self._initialize_exchange(exchange_id, api_key, api_secret, test_mode)
            self._initialize_managers()
            self._initialize_state()
            self._init_daily_summary()
            self._setup_signal_handlers()
            
            self.logger.info("Trading Bot initialized successfully")
            
        except Exception as e:
            self.logger.critical(f"Failed to initialize bot: {str(e)}")
            raise

    def _initialize_state(self):
        """Initialize bot state variables"""
        self.is_running = False
        self.shutdown_requested = False
        self.error_count = 0
        self.max_consecutive_errors = 5
        self.active_trades = {}
        self.last_status_update = 0
        self.status_lock = threading.Lock()

   

    def _log_status_update(self):
        """Log periodic status updates"""
        current_time = time.time()
        
        with self.status_lock:
            if current_time - self.last_status_update >= EXCHANGE_CONFIG['status_interval']:
                summary = self.trade_manager.get_performance_summary()
                
                self.logger.info("=== Status Update ===")
                self.logger.info(f"Active Trades: {len(self.active_trades)}")
                self.logger.info(f"Daily P&L: {summary['total_pnl']:.2f} USDC")
                self.logger.info(f"Current Drawdown: {summary['max_drawdown']:.1f}%")
                self.logger.info("===================")
                
                self.last_status_update = current_time

    def _scan_for_opportunities(self):
        """Scan market for trading opportunities"""
        try:
            for symbol in EXCHANGE_CONFIG['pairs']:
                # Skip if already trading this pair
                if symbol in self.active_trades:
                    continue
                    
                # Get market data
                ticker = self.market_manager.get_ticker(symbol)
                if not ticker:
                    continue
                
                # Basic price analysis
                current_price = float(ticker['last'])
                pair_config = EXCHANGE_CONFIG['pairs'][symbol]
                
                # Get signals
                volume_ok, volume_score = self.market_manager.analyze_volume(symbol)
                dex_signal, dex_confidence = self.market_manager.analyze_dex_array(symbol)
                
                # Check signal criteria
                if (volume_ok and 
                    volume_score >= pair_config['min_volume_score'] and
                    dex_signal and 
                    dex_confidence >= pair_config['min_signal_confidence']):
                    
                    self.logger.info(
                        f"Found opportunity for {symbol} "
                        f"(Volume Score: {volume_score:.2f}, "
                        f"Signal Confidence: {dex_confidence:.2f})"
                    )
                    
                    # Calculate position size
                    stop_loss = current_price * (1 - EXCHANGE_CONFIG['stop_loss'])
                    position_size = self.trade_manager.calculate_position_size(
                        symbol, current_price, stop_loss
                    )
                    
                    if position_size:
                        # Enter position
                        order = self.trade_manager.place_order(
                            symbol=symbol,
                            side='buy',
                            amount=position_size,
                            price=current_price
                        )
                        
                        if order:
                            # Store position info
                            self.active_trades[symbol] = {
                                'direction': 'buy',
                                'position_size': position_size,
                                'entry_price': current_price,
                                'stop_loss': stop_loss,
                                'take_profit': current_price * (1 + EXCHANGE_CONFIG['take_profit']),
                                'entry_time': time.time()
                            }
                
        except Exception as e:
            self.logger.error(f"Error scanning opportunities: {str(e)}")


    
    
    
    def _initialize_exchange(self, exchange_id: str, api_key: str, api_secret: str, test_mode: bool):
        """Initialize exchange connection"""
        try:
            exchange_class = getattr(ccxt, exchange_id)
            self.exchange = exchange_class({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'spot',
                    'recvWindow': EXCHANGE_CONFIG['recvWindow'],
                    'adjustForTimeDifference': True,
                    'createMarketBuyOrderRequiresPrice': False
                },
                'timeout': SYSTEM_CONFIG['request_timeout'] * 1000
            })
            
            if test_mode:
                self.exchange.set_sandbox_mode(True)
                self.logger.info("Running in test mode")
            
            # Load markets to validate connection
            self.exchange.load_markets()
            
        except Exception as e:
            raise APIError(f"Failed to initialize exchange: {str(e)}")
    
    def _initialize_managers(self):
        """Initialize market data and trade managers"""
        try:
            self.market_manager = MarketDataManager(
                self.exchange,
                EXCHANGE_CONFIG['pairs'],
                self.logger
            )
            
            self.trade_manager = TradeManager(
                self.exchange,
                EXCHANGE_CONFIG['pairs'],
                EXCHANGE_CONFIG['risk_amount'],
                self.logger
            )
            
        except Exception as e:
            raise ConfigError(f"Failed to initialize managers: {str(e)}")

def _manage_active_trades(self):
    """Manage active trades and update positions"""
    try:
        for symbol, position in list(self.active_trades.items()):
            # Get current market data
            ticker = self.market_manager.get_ticker(symbol)
            if not ticker:
                continue
                
            current_price = float(ticker['last'])
            
            # Check stop loss
            if position['stop_loss'] >= current_price:
                self.logger.info(f"Stop loss triggered for {symbol}")
                self.trade_manager.close_position(symbol, position)
                del self.active_trades[symbol]
                continue
                
            # Check take profit
            if position['take_profit'] <= current_price:
                self.logger.info(f"Take profit triggered for {symbol}")
                self.trade_manager.close_position(symbol, position)
                del self.active_trades[symbol]
                continue
                
    except Exception as e:
        self.logger.error(f"Error managing active trades: {str(e)}")

def _generate_daily_report(self):
    """Generate end-of-day trading report"""
    try:
        summary = self.trade_manager.get_performance_summary()
        
        self.logger.info("=== Daily Trading Report ===")
        self.logger.info(f"Date: {self.daily_summary['date']}")
        self.logger.info(f"Total Trades: {summary['total_trades']}")
        self.logger.info(f"Win Rate: {summary['win_rate']:.1f}%")
        self.logger.info(f"Total P&L: {summary['total_pnl']:.2f} USDC")
        self.logger.info(f"Max Drawdown: {summary['max_drawdown']:.1f}%")
        self.logger.info("=========================")
        
        return summary
        
    except Exception as e:
        self.logger.error(f"Error generating daily report: {str(e)}")
        return {}
    
    def _init_daily_summary(self):
        """Initialize daily performance tracking"""
        self.daily_summary = {
            'date': date.today(),
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_pnl': 0.0,
            'best_trade_pnl': 0.0,
            'worst_trade_pnl': 0.0,
            'avg_trade_duration': 0.0,
            'active_pairs': set(),
            'failed_executions': 0,
            'errors': [],
            'start_balance': self.trade_manager._get_account_balance(),
            'current_drawdown': 0.0,
            'max_drawdown': 0.0,
            'recovery_needed': False
        }
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        self.logger.info("Shutdown signal received. Cleaning up...")
        self.shutdown_requested = True
        self.stop()
    
    def _handle_day_change(self) -> bool:
        """Handle day change and reporting"""
        try:
            self.logger.info("Processing day change...")
            
            # Generate end of day report
            self._generate_daily_report()
            
            # Store previous day's summary
            self._store_daily_summary()
            
            # Reset daily tracking
            self._init_daily_summary()
            
            # Reset daily loss tracking
            self.trade_manager.daily_loss = 0.0
            
            # Check for drawdown threshold
            if self.trade_manager.max_drawdown >= RISK_CONFIG['max_drawdown']:
                self.logger.warning(
                    f"Maximum drawdown threshold reached: {self.trade_manager.max_drawdown:.2%}. "
                    "Stopping trading until recovery."
                )
                self.daily_summary['recovery_needed'] = True
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling day change: {str(e)}")
            return False
    
    def _check_recovery_conditions(self) -> bool:
        """Check if bot can resume trading after drawdown"""
        if not self.daily_summary['recovery_needed']:
            return True
            
        try:
            current_balance = self.trade_manager._get_account_balance()
            recovery_ratio = (current_balance - self.daily_summary['start_balance']) / \
                           abs(self.daily_summary['start_balance'])
            
            if recovery_ratio >= RISK_CONFIG['recovery_threshold']:
                self.logger.info(
                    f"Recovery threshold met ({recovery_ratio:.1%}). Resuming trading."
                )
                self.daily_summary['recovery_needed'] = False
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking recovery conditions: {str(e)}")
            return False
    
    def run(self):
        """Main bot loop with enhanced error handling"""
        self.is_running = True
        consecutive_errors = 0
        
        self.logger.info(
            f"Bot started - Trading pairs: {list(EXCHANGE_CONFIG['pairs'].keys())}"
        )
        
        while self.is_running and not self.shutdown_requested:
            try:
                # Check for new day
                if date.today() != self.daily_summary['date']:
                    if not self._handle_day_change():
                        break
                
                # Check recovery status if needed
                if self.daily_summary['recovery_needed']:
                    if not self._check_recovery_conditions():
                        time.sleep(60)  # Check every minute
                        continue
                
                # Update market data
                if not self.market_manager.update_market_data():
                    raise MarketDataError("Failed to update market data")
                
                # Process active trades
                self._manage_active_trades()
                
                # Look for new opportunities
                if (len(self.active_trades) < EXCHANGE_CONFIG['max_trades'] and 
                    not self.daily_summary['recovery_needed']):
                    self._scan_for_opportunities()
                
                # Log status update
                self._log_status_update()
                
                # Reset error counter on successful iteration
                consecutive_errors = 0
                
                # Main loop delay
                time.sleep(EXCHANGE_CONFIG['update_interval'])
                
            except Exception as e:
                consecutive_errors += 1
                self.error_count += 1
                self.daily_summary['failed_executions'] += 1
                self.daily_summary['errors'].append(str(e))
                
                self.logger.error(f"Error in main loop: {str(e)}")
                
                if consecutive_errors >= self.max_consecutive_errors:
                    self.logger.critical(
                        f"Too many consecutive errors ({consecutive_errors}), stopping bot"
                    )
                    self.stop()
                    break
                
                time.sleep(5)  # Error delay
                
        self.logger.info("Bot stopped")
    
    def stop(self):
        """Stop bot and cleanup"""
        self.logger.info("Stopping bot...")
        self.is_running = False
        
        try:
            # Close all positions
            self._close_all_positions()
            
            # Cleanup managers
            self.market_manager.cleanup()
            self.trade_manager.cleanup()
            
            # Generate final report
            self._generate_daily_report()
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
    
    def _close_all_positions(self):
        """Close all open positions"""
        try:
            for symbol, position in list(self.active_trades.items()):
                self.logger.info(f"Closing position for {symbol}")
                if self.trade_manager.close_position(symbol, position, market_close=True):
                    del self.active_trades[symbol]
                
        except Exception as e:
            self.logger.error(f"Error closing positions: {str(e)}")

if __name__ == "__main__":
    try:
        # Create required directories
        create_required_directories()
        
        # Setup error reporting
        setup_error_reporting()
        
        # Setup logging
        logger = setup_logger()
        logger.info("Starting trading bot initialization...")
        
        # Log system information
        logger.info(f"Python version: {sys.version}")
        external_ip = get_external_ip()
        if external_ip:
            logger.info(f"External IP: {external_ip}")
        
        # Load and validate configuration
        config_path = os.getenv('BOT_CONFIG', 'config/config.yaml')
        logger.info(f"Loading configuration from {config_path}")
        
        config = load_config(config_path)
        validate_config(config)
        
        # Initialize bot
        bot = TradingBot(
            exchange_id=EXCHANGE_CONFIG['exchange_id'],
            api_key=EXCHANGE_CONFIG['api_key'],
            api_secret=EXCHANGE_CONFIG['api_secret'],
            test_mode=EXCHANGE_CONFIG.get('test_mode', False)
        )
        
        # Run bot
        logger.info("Starting bot execution...")
        bot.run()
        
    except KeyboardInterrupt:
        logger.info("\nBot stopped by user")
        if 'bot' in locals():
            bot.stop()
        sys.exit(0)
        
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        if 'bot' in locals():
            bot.stop()
        sys.exit(1)

"""
Usage Instructions:
1. Install required packages:
   pip install ccxt pandas numpy pyyaml requests

2. Create config/config.yaml with your settings or use defaults

3. Set environment variables (optional):
   export BOT_CONFIG=/path/to/config.yaml

4. Run the bot:
   python trading_bot.py

Features:
- Market order execution for reliable fills
- Dynamic position sizing based on volatility
- Comprehensive risk management
- Detailed performance tracking
- Enhanced error handling
- Secure file operations
- Graceful shutdown handling
"""
