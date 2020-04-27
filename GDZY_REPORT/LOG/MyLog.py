
import logging
import sys
from os import makedirs
from os.path import dirname, exists
loggers = {}

class logger():

    def get_logger(name=None,level='debug',log_path = r'D:\txh_py\GDZY_REPORT\LOG/log.txt'):
        # 日志文件路径
        # 日志级别
        LOG_LEVEL = {
            'debug':logging.DEBUG,
            'info':logging.INFO,
            'warning':logging.WARNING,
            'error':logging.ERROR,
            'crit':logging.CRITICAL
        }

        LOG_FORMAT = '%(asctime)s - %(message)s'  # 每条日志输出格式
        #LOG_FORMAT = '%(message)s'  # 每条日志输出格式
        global loggers

        if not name: name = __name__

        if loggers.get(name):
            return loggers.get(name)
        logger = logging.getLogger(name)
        logger.setLevel(LOG_LEVEL.get(level))


        # 输出到文件
        log_dir = dirname(log_path)
        # 如果路径不存在，创建日志文件文件夹
        if not exists(log_dir): makedirs(log_dir)
        # 添加 FileHandler
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(level=LOG_LEVEL.get(level))
        formatter = logging.Formatter(LOG_FORMAT)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 输出到控制台
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level=LOG_LEVEL.get(level))
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # 保存到全局 loggers
        loggers[name] = logger
        return logger







#logger.get_logger().info("需要处理的数据是:" +  str(1) +   "条")


