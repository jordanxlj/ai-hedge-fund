import logging
import logging.config
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
        print('ai hedge fund logger setup failed')