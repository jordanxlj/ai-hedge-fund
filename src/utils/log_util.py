import logging

def logger_setup():
    # 创建一个自定义日志记录器
    logger = logging.getLogger(__name__)

    # 创建一个文件处理器，将日志写入文件
    handler = logging.FileHandler('ai-hedge-fund.log')

    # 设置日志消息的格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s')
    handler.setFormatter(formatter)

    # 将处理器添加到日志记录器
    logger.addHandler(handler)

    # 设置日志级别
    logger.setLevel(logging.DEBUG)

    # 使用自定义日志记录器记录消息
    logger.info('-------------ai hedge fund start---------------')