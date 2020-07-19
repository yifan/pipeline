class PipelineError(Exception):
    def __init__(self, message, data=None, traceback=None):
        super().__init__(message)
        self.data = data
        self.traceback = traceback

    def log(self, logger):
        if self.data:
            if hasattr(self.data, 'log'):
                self.data(logger)
            else:
                logger.warning(self.data)
        if self.traceback:
            logger.warning(self.traceback)
