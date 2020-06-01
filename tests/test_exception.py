import logging
from unittest import TestCase

from pipeline import PipelineError


class TestException(TestCase):
    def test_default(self):
        try:
            raise PipelineError("Test")
        except PipelineError as e:
            e.log(logging.getLogger('test'))
