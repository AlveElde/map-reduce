from joblib import Parallel, delayed
import functools
import hashlib
from typing import Iterable, Iterator

from .corpus import InMemoryDocument, InMemoryCorpus
from .invertedindex import InMemoryInvertedIndex
from .normalization import BrainDeadNormalizer
from .tokenization import BrainDeadTokenizer
from .dictionary import InMemoryDictionary

class IntermediatePosting:
    #TODO: slots
    def __init__(self, term: str, document_id: int):
        self.term = term
        self.document_id = document_id

class MapReduceJob:
    def __init__(self):
        self._corpus = InMemoryCorpus()
        self._normalizer = BrainDeadNormalizer()
        self._tokenizer = BrainDeadTokenizer()
        self._corpus.add_document(InMemoryDocument(0, {"body": "this is a Test"}))
        self._corpus.add_document(InMemoryDocument(1, {"body": "test TEST prØve"}))
        self._index = InMemoryInvertedIndex(self._corpus, ["body"], self._normalizer, self._tokenizer)
        self._fields = ["body"]

    def mapreduce(self):
        mappers = 2
        reducers = 2
        results = self.parallelize(self.map_terms, self._corpus._documents, mappers)

        # Partition
        #TODO: Make parallel
        partitions = [[] for i in range(0, reducers)]

        for interpostings in results:
            for interposting in interpostings:
                partitions[hash(interposting.term) % reducers] = interposting
        
        #postings = self.parallelize(self.reduce_to_postings, partitions, reducers)

    def parallelize(self, func, elements, jobs):
        return Parallel(n_jobs=jobs)(delayed(func)(elem) for elem in elements)

    def map_terms(self, doc):
        # Take a list of documents
        # Create a (term, doc_id) for each term in each field
        # Return a list of (term, doc_id) tuples
        mapped_items = []
        for field in self._fields:
            for term in self._index.get_terms(doc[field]):
                mapped_items.append(IntermediatePosting(term, doc.document_id))
        return mapped_items

    
    def partition(self):
        # Iterate through each term
        # worker to assign = hash of term % number of workers
        # Place in array of correct worker

        pass

    def reduce_to_postings(self, interpostings):
        # Take a list of (term, doc_id) tuples
        # Group the tuples on term
        # Return a list of postings
        self._dictionary = InMemoryDictionary()
        self._posting_lists = []

        for interposting in interpostings:
            term_id = self._dictionary.add_if_absent(term)
            self._posting_lists.extend([] for i in range(len(self._posting_lists), term_id+1))
            self._posting_lists[term_id].append(Posting(doc.document_id, counter[term]))
