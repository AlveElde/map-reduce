from .corpus import InMemoryDocument, InMemoryCorpus
from .normalization import BrainDeadNormalizer
from .tokenization import BrainDeadTokenizer
from .dictionary import InMemoryDictionary
from .posting import Posting

from typing import Iterable, Iterator

from joblib import Parallel, delayed

class MapReducer:
    def __init__(self, fields, corpus, normalizer, tokenizer):
        self._fields = fields
        self._corpus = corpus
        self._normalizer = normalizer
        self._tokenizer = tokenizer

    def mapreduce(self, mappers, reducers, print_log) -> (list, dict):
        self.print_log("Starting mapping...", print_log)
        keyvals_list = self._parallelize(self._map, self._corpus._documents, mappers)
        self.print_log("Finished mapping!", print_log)

        self.print_log("Starting partitioning...", print_log)
        parts = self._partition(keyvals_list, reducers)
        self.print_log("Finished partitioning!", print_log)
       
        self.print_log("Starting reducing...", print_log)
        reduced_parts = self._parallelize(self._reduce, parts, reducers)
        self.print_log("Finished reducing!", print_log)

        self.print_log("Starting combining...", print_log)
        combined_parts = self._combine(reduced_parts)
        self.print_log("Finished combining!", print_log)

        return combined_parts
    
    def print_log(self, log_line, print_log):
        if print_log:
            print(log_line)

    def _parallelize(self, func, parts, total_parts):
        return Parallel(n_jobs=total_parts, prefer="threads")(delayed(func)(part) for part in parts)

    def get_terms(self, buffer: str) -> Iterator[str]:
        return (self._normalizer.normalize(t) for t in self._tokenizer.strings(self._normalizer.canonicalize(buffer)))

    def _map(self, part):
        keyvals = []
        for field in self._fields:
            for term in self.get_terms(part[field]):
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
