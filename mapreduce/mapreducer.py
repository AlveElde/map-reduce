import itertools
from typing import Iterable, Iterator
from joblib import Parallel, delayed

from .corpus import Document, Corpus, InMemoryDocument, InMemoryCorpus
from .normalization import Normalizer, BrainDeadNormalizer
from .tokenization import Tokenizer,  BrainDeadTokenizer
from .dictionary import InMemoryDictionary
from .posting import Posting


class MapReducer:
    def __init__(self, fields : Iterable[str], corpus: Corpus, normalizer: Normalizer, tokenizer: Tokenizer):
        self._fields = fields
        self._corpus = corpus
        self._normalizer = normalizer
        self._tokenizer = tokenizer
    
    # Build an inverted index with MapReduce semantics.
    def mapreduce(self, mappers: int, reducers: int, print_log: bool) -> (list, dict):
        self._print_log("Starting mapping...", print_log)
        keyvals_list = self._parallelize(self._map, self._corpus._documents, mappers)
        self._print_log("Finished mapping!", print_log)

        self._print_log("Starting partitioning...", print_log)
        parts = self._partition(keyvals_list, reducers)
        self._print_log("Finished partitioning!", print_log)
       
        self._print_log("Starting reducing...", print_log)
        reduced_parts = self._parallelize(self._reduce, parts, reducers)
        self._print_log("Finished reducing!", print_log)

        self._print_log("Starting combining...", print_log)
        combined_parts = self._combine(reduced_parts)
        self._print_log("Finished combining!", print_log)

        return combined_parts
    

    # Print log if desired.
    def _print_log(self, log_line, print_log):
        if print_log:
            print(log_line)
        

    # Run the function func on each part in parts.
    # JobLib will perform the function calls in parallel threads.
    # This becomes very slow for a large number of parts.
    def _parallelize(self, func: callable, elements: list, total_parts: int) -> list:
        parts = []
        for i in range(0, len(elements), total_parts):
            parts.append(elements[i:i + total_parts])

        return Parallel(n_jobs=total_parts, prefer="threads", backend="threading")(
            delayed(func)(part)
            for part in parts)


    # Helper function to normalize and tokenize a buffer. 
    def _get_terms(self, buffer: str) -> Iterator[str]:
        return (self._normalizer.normalize(t) for t in self._tokenizer.strings(self._normalizer.canonicalize(buffer)))


    # Create (key, value) tuples for each term in a document.
    # The term is the key and the document ID is the value.
    # Terms are not counted in any way here. 
    def _map(self, docs: list) -> list:
        keyvals = []
        for doc in docs:
            for field in self._fields:
                for term in self._get_terms(doc[field]):
                    keyvals.append((term, doc.document_id))
        return keyvals


    # Partition the (key, value) pairs into a number of partitions.
    # The terms are partitioned based on a modulated hash of each term.
    # Hashing each term ensures all key/val pairs of that term are put in the same partition.
    # This will probably result in a non-optimal distribution of terms with high frequency. 
    def _partition(self, keyvals_list: list, total_parts: int) -> list:
        parts = [[] for i in range(0, total_parts)]
        for keyvals in keyvals_list:
            for term, doc_id in keyvals:
                parts[hash(term) % total_parts].append((term, doc_id))
        return parts


    # Reduce the (key, value) pairs into posting lists.
    # This step will produce complete posting lists for each term in this partition.
    # It will also produce a term ID dictionary for each term in this partition.
    def _reduce(self, partitions: list) -> (list, dict):
        dictionary = InMemoryDictionary()
        posting_lists = []

        for partition in partitions:
            for term, doc_id in partition:
                term_id = dictionary.add_if_absent(term)
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


    # Combine the dictionaries from each reduced partition and join the posting lists.
    # The term ID of a posting list must be offset by the posting lists from other
    # partitions before it.
    def _combine(self, reduced_parts : list) -> (list, dict):
        combined_posting_lists = []
        combined_dictionary = {}

        for posting_lists, dictionary in reduced_parts:
            for term, term_id in dictionary:
                combined_dictionary[term] = len(combined_posting_lists) + term_id
            combined_posting_lists.extend(posting_lists)

        return combined_posting_lists, combined_dictionary
