#!/usr/bin/env python3
"""超时和重试装饰器模块"""

import time
import socket
import logging
from functools import wraps
from typing import Any, Callable

logger = logging.getLogger(__name__)


def with_timeout_retry(interface_name: str):
    """装饰器：为Tushare等数据提供商添加超时和重试机制，从配置中读取参数"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            from src.data.data_config import get_timeout_seconds, get_max_retries, get_retry_delay
            
            timeout_seconds = get_timeout_seconds(interface_name)
            max_retries = get_max_retries(interface_name)
            retry_delay = get_retry_delay(interface_name)
            
            for attempt in range(max_retries):
                try:
                    # 设置pandas的网络超时
                    original_timeout = socket.getdefaulttimeout()
                    socket.setdefaulttimeout(timeout_seconds)
                    
                    result = func(*args, **kwargs)
                    return result
                    
                except (Exception, socket.timeout) as e:
                    if attempt == max_retries - 1:  # 最后一次尝试
                        logger.error(f"函数 {func.__name__} 在 {max_retries} 次重试后仍然失败: {e}")
                        raise e
                    else:
                        logger.warning(f"函数 {func.__name__} 第 {attempt + 1} 次尝试失败: {e}, 将在 {retry_delay} 秒后重试")
                        time.sleep(retry_delay)
                finally:
                    # 重置超时设置
                    socket.setdefaulttimeout(original_timeout)
            return None
        return wrapper
    return decorator


def with_http_timeout_retry(interface_name: str):
    """装饰器：为HTTP请求添加超时和重试机制，从配置中读取参数"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            from src.data.data_config import get_timeout_seconds, get_max_retries, get_retry_delay
            
            timeout_seconds = get_timeout_seconds(interface_name)
            max_retries = get_max_retries(interface_name)
            retry_delay = get_retry_delay(interface_name)
            
            for attempt in range(max_retries):
                try:
                    # 不向被装饰的函数传递timeout参数
                    # 超时在FinancialDatasetsProvider的_make_request方法中处理
                    result = func(*args, **kwargs)
                    return result
                    
                except Exception as e:
                    # 只对网络相关错误进行重试
                    if attempt == max_retries - 1:  # 最后一次尝试
                        logger.error(f"函数 {func.__name__} 在 {max_retries} 次重试后仍然失败: {e}")
                        raise e
                    
                    # 检查是否为可重试的错误
                    error_msg = str(e).lower()
                    retryable_errors = ['timeout', 'connection', 'network', 'temporary', 'too many requests']
                    if any(err in error_msg for err in retryable_errors):
                        logger.warning(f"函数 {func.__name__} 第 {attempt + 1} 次尝试失败: {e}, 将在 {retry_delay} 秒后重试")
                        time.sleep(retry_delay)
                    else:
                        # 非网络错误，直接抛出
                        logger.error(f"函数 {func.__name__} 遇到非网络错误: {e}")
                        raise e
            return None
        return wrapper
    return decorator


def with_llm_timeout_retry(interface_name: str):
    """装饰器：为LLM调用添加超时和重试机制，从配置中读取参数"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            from src.data.data_config import get_timeout_seconds, get_max_retries, get_retry_delay
            
            timeout_seconds = get_timeout_seconds(interface_name)
            max_retries = get_max_retries(interface_name)
            retry_delay = get_retry_delay(interface_name)
            
            for attempt in range(max_retries):
                try:
                    # 将超时设置添加到kwargs中
                    if 'timeout' not in kwargs:
                        kwargs['timeout'] = timeout_seconds
                    
                    result = func(*args, **kwargs)
                    return result
                    
                except Exception as e:
                    if attempt == max_retries - 1:  # 最后一次尝试
                        logger.error(f"LLM调用 {func.__name__} 在 {max_retries} 次重试后仍然失败: {e}")
                        raise e
                    
                    # 检查是否为可重试的错误
                    error_msg = str(e).lower()
                    retryable_errors = ['timeout', 'rate limit', 'too many requests', 'server error', 'temporary', 'connection']
                    if any(err in error_msg for err in retryable_errors):
                        logger.warning(f"LLM调用 {func.__name__} 第 {attempt + 1} 次尝试失败: {e}, 将在 {retry_delay} 秒后重试")
                        time.sleep(retry_delay)
                    else:
                        # 非可重试错误，直接抛出
                        logger.error(f"LLM调用 {func.__name__} 遇到非可重试错误: {e}")
                        raise e
            return None
        return wrapper
    return decorator 