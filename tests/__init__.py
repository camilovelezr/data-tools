from unittest import TestSuite
from .test_pipelines import TestPipelines

test_cases = (TestPipelines,)


def load_tests(loader, tests, pattern):
    suite = TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
