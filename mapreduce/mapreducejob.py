from .corpus import InMemoryDocument, InMemoryCorpus
from .invertedindex import InMemoryInvertedIndex
from .normalization import BrainDeadNormalizer
from .tokenization import BrainDeadTokenizer
from .dictionary import InMemoryDictionary
from .invertedindex import Posting

from joblib import Parallel, delayed

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

        # Map.
        keyvals_list = self._parallelize(self._map, self._corpus._documents, mappers)

        # Partition.
        parts = self._partition(keyvals_list, reducers)
       
        # Reduce.
        reduced_parts = self._parallelize(self._reduce, parts, reducers)

        # Combine.
        combined_posting_lists, combined_dictionary = self._combine(reduced_parts)

        print("Done!")

    def _parallelize(self, func, parts, total_parts):
        return Parallel(n_jobs=total_parts)(delayed(func)(part) for part in parts)


    def _map(self, part):
        keyvals = []
        for field in self._fields:
            for term in self._index.get_terms(part[field]):
                keyvals.append((term, part.document_id))
        return keyvals
    

    def _partition(self, keyvals_list, total_parts):
        parts = [[] for i in range(0, total_parts)]
        for keyvals in keyvals_list:
            for term, doc_id in keyvals:
                parts[hash(term) % total_parts].append((term, doc_id))
        return parts


    def _reduce(self, part):
        dictionary = InMemoryDictionary()
        posting_lists = []

        for term, doc_id in part:
            term_id = dictionary.add_if_absent(term)

            # Ensure we have a place to put our posting lists.
            posting_lists.extend([] for i in range(len(posting_lists), term_id+1))

            # Increment the count on a previously existing posting for this document ID. 
            has_posting = False
            for posting in posting_lists[term_id]:
                if posting.document_id == doc_id:
                    posting.term_frequency += 1
                    has_posting = True
                    break
            
            # Create a new posting for this document ID.
            if not has_posting:
                posting_lists[term_id].append(Posting(doc_id, 1))
        
        return posting_lists, dictionary


    def _combine(self, reduced_parts):
        combined_posting_lists = []
        combined_dictionary = {}

        for posting_lists, dictionary in reduced_parts:
            for term, term_id in dictionary:
                combined_dictionary[term] = len(combined_posting_lists) + term_id
            combined_posting_lists.extend(posting_lists)

        return combined_posting_lists, combined_dictionary
