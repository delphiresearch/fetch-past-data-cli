import os
import logging
from datetime import datetime


def setup_logger(name: str, log_dir: str = None) -> logging.Logger:
    """
    Sets up a logger instance
    
    Args:
        name: Logger name (used as prefix for log filename)
        log_dir: Directory path to save log files. If None, uses default log directory
        
    Returns:
        Configured logger instance
    """
    # Set default log directory
    if log_dir is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(project_root, "output", "log")
    
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate log filename (e.g., market_errors_20240321_123456.txt)
    log_file = os.path.join(
        log_dir,
        f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    
    # Get logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Configure file handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # Create formatter
    formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    
    # Add file handler to logger
    logger.addHandler(file_handler)
    
    return logger