"""E2ETabularMLPipeline
"""

__version__ = "0.1"

from abc import ABC, abstractmethod
import logging
import time


class BaseClass(ABC):

    def setup(self):
        self.logger = logging.getLogger(BaseClass.__name__)
        self.start_time = time.time()
        self.end_time = None

    @abstractmethod
    def core(self):
        pass

    def teardown(self):
        self.end_time = time.time()
        hours, rem = divmod(self.end_time - self.start_time, 3600)
        minutes, seconds = divmod(rem, 60)
        self.logger.info("Total time taken : {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))

    def run(self):
        self.setup()
        self.core()
        self.teardown()
