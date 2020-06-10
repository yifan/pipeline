import os
import tempfile
from unittest import TestCase

from pipeline import Message, Generator, Processor, Splitter


class TestWorkerCore(TestCase):
    def setUp(self):
        self.testDir = tempfile.TemporaryDirectory()
        self.infile = open(os.path.join(self.testDir.name, 'infile.txt'), 'w')
        self.infile.write("""[{},{"key":1, "language":"en"}]
                             [{},{"key":2, "language":"it"}]
                             [{},{"key":3, "language":"ar"}]""")
        self.infile.close()

    def test_mem_generator(self):
        class MyGenerator(Generator):
            def generate(self):
                for i in range(3):
                    yield {"key": i}
        generator = MyGenerator('generator', '0.1.0')
        generator.parse_args(args=['--kind', 'MEM', '--out-topic', 'test'])
        generator.start()
        assert len(generator.destination.results) == 3
        m = generator.destination.results[0]
        dct = m.get_version('generator')
        assert dct['version'] == [0, 1, 0]
        assert dct['order'] == 1

    def test_mem_generator_with_data_writer(self):
        class MyGenerator(Generator):
            def generate(self):
                for i in range(3):
                    self.dataWriter.write(i, dict([(field, field) for field in self.dataWriter.fields]))
                    yield {"key": i}
        generator = MyGenerator('generator', '0.1.0', dataKind='MEM')
        generator.parse_args(args=['--kind', 'MEM', '--out-topic', 'test', '--out-fields', 'key1,key2'])
        generator.start()
        assert len(generator.dataWriter.results) == 3
        for i in range(3):
            assert i in generator.dataWriter.results
        assert generator.dataWriter.results[0]['key1'] == 'key1'

    def test_generator_invalid_message(self):
        class InvalidMessage(Message):
            def is_valid(self):
                return False

        class MyGenerator(Generator):
            def generate(self):
                for i in range(3):
                    yield {"key": i}

        generator = MyGenerator('generator', '0.1.0', messageClass=InvalidMessage)
        generator.parse_args(args=['--kind', 'MEM', '--out-topic', 'test'])
        generator.start()
        assert len(generator.destination.results) == 0

    def test_processor_invalid_message(self):
        class InvalidMessage(Message):
            def is_valid(self):
                return False

        msgs = [{}, {}, {}]
        pro1 = Processor('tester1', '0.1.0', messageClass=InvalidMessage)
        pro1.parse_args(args=['--kind', 'MEM', '--out-topic', 'test'], config={'data': msgs})
        pro1.use_retry_topic('optional-retry-topic')
        pro1.start()
        assert len(pro1.destination.results) == 0
        assert len(pro1.retryDestination.results) == 3

    def test_processor_retry(self):
        class RetryProcessor(Processor):
            def process(self, dct):
                return "Error"
        msgs = [{'key': '1'}, {'key': '2'}, {'key': '3'}]
        pro1 = RetryProcessor('tester1', '0.1.0')
        pro1.parse_args(args=['--kind', 'MEM', '--out-topic', 'test'], config={'data': msgs})
        pro1.use_retry_topic('optional-retry-topic')
        pro1.start()
        assert len(pro1.destination.results) == 0
        assert len(pro1.retryDestination.results) == 3

    def test_processor(self):
        class MyProcessor(Processor):
            def process(self, dct):
                dct['newkey'] = 'newval'
                return None
        msgs = [{'key': '1'}, {'key': '2'}, {'key': '3'}]
        pro1 = MyProcessor('tester1', '0.1.0')
        pro1.parse_args(args=['--kind', 'MEM', '--out-topic', 'test'], config={'data': msgs})
        pro1.start()
        assert len(pro1.destination.results) == 3
        m = pro1.destination.results[0]
        assert m.dct['newkey'] == 'newval'

    def test_processor_with_data(self):
        class MyProcessor(Processor):
            def process(self, dct):
                self.dataWriter.write(dct['key'], {'key3': self.dataReader.read(dct['key'])})
                return None
        msgs = [{'key': 'm1'}, {'key': 'm2'}, {'key': 'm3'}]
        pro1 = MyProcessor('tester1', '0.1.0', dataKind='MEM')
        pro1.parse_args(
            args='--kind MEM --out-topic test --in-fields key1,key2 --out-fields key3'.split(),
            config={
                'data': msgs,
                'mem': {
                    'm1': {'key1': 'val11', 'key2': 'val21'},
                    'm2': {'key1': 'val12', 'key2': 'val22'},
                    'm3': {'key1': 'val13', 'key2': 'val23'},
                }
            }
        )
        pro1.start()
        assert len(pro1.destination.results) == 3
        m = pro1.dataWriter.results['m1']
        assert m['key3']['key1'] == 'val11'
        assert m['key3']['key2'] == 'val21'

    def test_file(self):
        processor = Processor('fileprocessor', '0.1.0')
        processor.parse_args(args=['--kind', 'FILE', '--in-topic', 'test', '--out-topic', 'test',
                                   '--infile', os.path.join(self.testDir.name, 'infile.txt'),
                                   '--outfile', os.path.join(self.testDir.name, 'outfile.txt')])
        processor.start()
        with open(os.path.join(self.testDir.name, 'outfile.txt'), 'r') as f:
            assert len(list(f)) == 3

    def test_file_stdin_stdout(self):
        processor = Processor('fileprocessor', '0.1.0')
        processor.parse_args(args=['--kind', 'FILE', '--in-topic', 'test', '--out-topic', 'test',
                                   '--infile', os.path.join(self.testDir.name, 'infile.txt'),
                                   '--outfile', '-'])
        processor.start()

    def test_file_repeat(self):
        processor = Processor('fileprocessor', '0.1.0')
        processor.parse_args(args=['--kind', 'FILE', '--in-topic', 'test', '--out-topic', 'test',
                                   '--infile', os.path.join(self.testDir.name, 'infile.txt'),
                                   '--repeat', "2",
                                   '--outfile', os.path.join(self.testDir.name, 'outfile.txt')])
        processor.start()
        with open(os.path.join(self.testDir.name, 'outfile.txt'), 'r') as f:
            assert len(list(f)) == 6

    def test_mem_processor(self):
        msgs = [{}, {}, {}]
        pro1 = Processor('tester1', '0.1.0')
        pro1.parse_args(args=['--kind', 'MEM', '--out-topic', 'test'], config={'data': msgs})
        pro1.start()
        assert len(pro1.destination.results) == 3

    def test_mem_processor_nooutput(self):
        pro1 = Processor('tester2', '0.1.0', nooutput=True)
        pro1.parse_args(args=['--kind', 'MEM'], config={'data': [{}]})
        pro1.start()
        assert not hasattr(pro1, 'destination')

    def test_splitter(self):
        msgs = [{'language': 'en'}, {'language': 'it'}]
        splitter = Splitter('spliter1', '0.1.0')
        splitter.parse_args(args=['--kind', 'MEM', '--out-topic', 'test'], config={'data': msgs})
        splitter.start()
        assert len(splitter.destinations['test-en'].results) == 1
        assert splitter.destinations['test-en'].results[0].dct['language'] == 'en'
        assert len(splitter.destinations['test-it'].results) == 1
        assert splitter.destinations['test-it'].results[0].dct['language'] == 'it'

    def test_splitter_invalid_message(self):
        class InvalidMessage(Message):
            def is_valid(self):
                return False

        msgs = [{"key": 1, 'language': 'en'}, {"key": 2, 'language': 'it'}]
        splitter = Splitter('spliter1', '0.1.0', messageClass=InvalidMessage)
        splitter.parse_args(args=['--kind', 'MEM', '--out-topic', 'test'], config={'data': msgs})
        splitter.start()
        assert len(splitter.destinations['test-en'].results) == 0
        assert len(splitter.destinations['test-it'].results) == 0

    def test_splitter_file(self):
        splitter = Splitter('spliter1', '0.1.0')
        splitter.parse_args(args=['--kind', 'FILE', '--in-topic', 'test', '--out-topic', 'test',
                                  '--infile', os.path.join(self.testDir.name, 'infile.txt'),
                                  '--outfile', os.path.join(self.testDir.name, 'outfile.txt')])
        splitter.start()
        assert len(splitter.destinations) == 3

    def test_custom_message(self):
        class CustomMessage(Message):
            def __str__(self):
                return 'Message({}: {})'.format(self.dct['key'], self.dct['value'])
        pro1 = Processor('tester3', '0.1.0', messageClass=CustomMessage)
        pro1.parse_args(args=['--kind', 'MEM'], config={'data': [{'key': 'key1', 'value': 'value1'}]})
        pro1.start()
        assert pro1.destination.results[0].dct['key'] == 'key1'
