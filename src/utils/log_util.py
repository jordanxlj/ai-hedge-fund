import logging
import logging.config
import os
from pathlib import Path

def logger_setup():
    """使用log.ini配置文件初始化日志系统"""
    # 获取项目根目录路径
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    log_config_path = project_root / "conf" / "log.ini"
    
    if log_config_path.exists():
        # 使用配置文件初始化日志
        logging.config.fileConfig(log_config_path)
        logger = logging.getLogger(__name__)
        logger.info('-------------ai hedge fund start---------------')
    else:
        # 如果配置文件不存在，回退到默认配置
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s',
            handlers=[
                logging.FileHandler('ai-hedge-fund.log'),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger(__name__)
        logger.warning(f'日志配置文件 {log_config_path} 不存在，使用默认配置')
        logger.info('-------------ai hedge fund start---------------')