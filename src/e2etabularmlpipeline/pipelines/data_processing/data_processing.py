from e2etabularmlpipeline import BaseClass
import logging

class DataProcessing(BaseClass):
    def __init__(self):
        self.logger = logging.getLogger(self.__name__)


    def core(self):
        self.logger.info("Running data processing core!!!")

    def