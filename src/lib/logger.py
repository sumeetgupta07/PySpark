# for universal  logging  to yarn logs
import logging


class Log4j:
    def __init__(self, spark):
        if spark is not None:
            log4j = spark._jvm.org.apache.log4j

            conf = spark.sparkContext.getConf()
            app_name = conf.get("spark.app.name")

            self.logger = log4j.LogManager.getLogger(app_name)
        else:
            self.logger = logging.getLogger("dummy")

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
