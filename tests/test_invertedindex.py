import unittest

class TestInMemoryInvertedIndex(unittest.TestCase):
    def setUp(self):
        from mapreduce.normalization import BrainDeadNormalizer
        from mapreduce.tokenization import BrainDeadTokenizer
        self._normalizer = BrainDeadNormalizer()
        self._tokenizer = BrainDeadTokenizer()

    def test_access_postings(self):
        from mapreduce.corpus import InMemoryDocument, InMemoryCorpus
        from mapreduce.invertedindex import InMemoryInvertedIndex
        corpus = InMemoryCorpus()
        corpus.add_document(InMemoryDocument(0, {"body": "this is a Test"}))
        corpus.add_document(InMemoryDocument(1, {"body": "test TEST prØve"}))
        index = InMemoryInvertedIndex(corpus, ["body"], self._normalizer, self._tokenizer)
        self.assertListEqual(list(index.get_terms("PRøvE wtf tesT")), ["prøve", "wtf", "test"])
        self.assertListEqual([(p.document_id, p.term_frequency) for p in index["prøve"]], [(1, 1)])
        self.assertListEqual([(p.document_id, p.term_frequency) for p in index.get_postings_iterator("wtf")], [])
        self.assertListEqual([(p.document_id, p.term_frequency) for p in index["test"]], [(0, 1), (1, 2)])
        self.assertEqual(index.get_document_frequency("wtf"), 0)
        self.assertEqual(index.get_document_frequency("prøve"), 1)
        self.assertEqual(index.get_document_frequency("test"), 2)

    def test_mesh_corpus(self):
        import os.path
        from mapreduce.corpus import InMemoryCorpus
        from mapreduce.invertedindex import InMemoryInvertedIndex

        corpus = InMemoryCorpus(os.path.join("../data", 'mesh.txt'))
        index = InMemoryInvertedIndex(corpus, ["body"], self._normalizer, self._tokenizer)
        self.assertEqual(len(list(index["hydrogen"])), 8)
        self.assertEqual(len(list(index["hydrocephalus"])), 2)

    def test_multiple_fields(self):
        from mapreduce.corpus import InMemoryDocument, InMemoryCorpus
        from mapreduce.invertedindex import InMemoryInvertedIndex
        document = InMemoryDocument(0, {
            'felt1': 'Dette er en test. Test, sa jeg. TEST!',
            'felt2': 'test er det',
            'felt3': 'test TEsT',
        })
        corpus = InMemoryCorpus()
        corpus.add_document(document)
        index = InMemoryInvertedIndex(corpus, ['felt1', 'felt3'], self._normalizer, self._tokenizer)
        posting = next(index.get_postings_iterator('test'))
        self.assertEqual(posting.document_id, 0)
        self.assertEqual(posting.term_frequency, 5)


if __name__ == '__main__':
    unittest.main()
