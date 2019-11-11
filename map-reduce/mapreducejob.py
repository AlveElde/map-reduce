from joblib import Parallel, delayed
import functools
import hashlib
from typing import Iterable, Iterator

from .corpus import InMemoryDocument, InMemoryCorpus
from .invertedindex import InMemoryInvertedIndex
from .normalization import BrainDeadNormalizer
from .tokenization import BrainDeadTokenizer
from .dictionary import InMemoryDictionary
from .invertedindex import Posting

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
        parts = [[] for i in range(0, reducers)]

        for interpostings in results:
            for interposting in interpostings:
                parts[hash(interposting.term) % reducers].append(interposting)
        
        results = self.parallelize(self.reduce_to_posting_lists, parts, reducers)

        combined_posting_lists = []
        combined_dictionary = {}

        for posting_lists, dictionary in results:
            for (term, term_id) in dictionary:
                combined_dictionary[term] = len(combined_posting_lists) + term_id

            combined_posting_lists.extend(posting_lists)


        
        print("Done!")


    def parallelize(self, func, parts, total_parts):
        return Parallel(n_jobs=total_parts)(delayed(func)(part) for part in parts)


    def map_terms(self, part):
        mapped_items = []
        for field in self._fields:
            for term in self._index.get_terms(part[field]):
                mapped_items.append(IntermediatePosting(term, part.document_id))
        return mapped_items

    
    def partition_ter(self):
        # Iterate through each term
        # worker to assign = hash of term % number of workers
        # Place in array of correct worker
        pass


    def reduce_to_posting_lists(self, part):
        dictionary = InMemoryDictionary()
        posting_lists = []

        for interposting in part:
            term_id = dictionary.add_if_absent(interposting.term)

            # Ensure we have a place to put our posting lists.
            posting_lists.extend([] for i in range(len(posting_lists), term_id+1))

            # Increment the count on a previously existing posting for this document ID. 
            has_posting = False
            for posting in posting_lists[term_id]:
                if posting.document_id == interposting.document_id:
                    posting.term_frequency += 1
                    has_posting = True
                    break
            
            # Create a new posting for this document ID.
            if not has_posting:
                posting_lists[term_id].append(Posting(interposting.document_id, 1))
        
        return posting_lists, dictionary
