import logging
from unittest import TestCase

from pipeline import PipelineException


class TestException(TestCase):
    def test_default(self):
        try:
            raise PipelineException("Test")
        except PipelineException as e:
            e.log(logging.getLogger('test'))
