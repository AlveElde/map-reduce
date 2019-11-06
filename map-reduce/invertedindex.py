#!/usr/bin/python
# -*- coding: utf-8 -*-

import itertools
from abc import ABC, abstractmethod
from .dictionary import InMemoryDictionary
from .normalization import Normalizer
from .tokenization import Tokenizer
from .corpus import Corpus
from collections import Counter
from typing import Iterable, Iterator


class Posting:
    """
    A very simple posting entry in a non-positional inverted index.
    """

    def __init__(self, document_id: int, term_frequency: int):
        self.document_id = document_id
        self.term_frequency = term_frequency

    def __repr__(self):
        return str({"document_id": self.document_id, "term_frequency": self.term_frequency})


class InvertedIndex(ABC):
    """
    Abstract base class for a simple inverted index.
    """

    def __getitem__(self, term: str) -> Iterator[Posting]:
        return self.get_postings_iterator(term)

    def __contains__(self, term: str) -> bool:
        return self.get_document_frequency(term) > 0

    @abstractmethod
    def get_terms(self, buffer: str) -> Iterator[str]:
        """
        Processes the given text buffer and returns an iterator that yields normalized
        terms as they are indexed. Both query strings and documents need to be
        identically processed.
        """
        pass

    @abstractmethod
    def get_postings_iterator(self, term: str) -> Iterator[Posting]:
        """
        Returns an iterator that can be used to iterate over the term's associated
        posting list. For out-of-vocabulary terms we associate empty posting lists.
        """
        pass

    @abstractmethod
    def get_document_frequency(self, term: str) -> int:
        """
        Returns the number of documents in the indexed corpus that contains the given term.
        """
        pass


class InMemoryInvertedIndex(InvertedIndex):
    """
    A simple in-memory implementation of an inverted index, suitable for small corpora.

    In a serious application we'd have configuration to allow for field-specific NLP,
    scale beyond current memory constraints, have a positional index, and so on.
    """

    def __init__(self, corpus: Corpus, fields: Iterable[str], normalizer: Normalizer, tokenizer: Tokenizer):
        self._corpus = corpus
        self._normalizer = normalizer
        self._tokenizer = tokenizer
        self._posting_lists = []
        self._dictionary = InMemoryDictionary()
        self._build_index(fields)

    def __repr__(self):
        return str({term: self._posting_lists[term_id] for (term, term_id) in self._dictionary})

    def _build_index(self, fields: Iterable[str]) -> None:
        for document in self._corpus:

            # Compute TF values for all unique terms in the document. Note that we
            # currently don't keep track of which field each term occurs in.
            # If we were to allow fields searches (e.g., "find documents that
            # contain 'foo' in the 'title' field") then we would have to keep
            # track of that, either as a synthetic term in the dictionary
            # (e.g., 'title.foo') or as extra data in the posting.
            all_terms = itertools.chain.from_iterable(self.get_terms(document.get_field(f, "")) for f in fields)
            term_frequencies = Counter(all_terms)

            for (term, term_frequency) in term_frequencies.items():

                # Assign the term an identifier, if needed. First come, first serve.
                term_id = self._dictionary.add_if_absent(term)

                # Locate the posting list for this term.
                if term_id >= len(self._posting_lists):
                    self._posting_lists.extend(([] for _ in range(len(self._posting_lists) - term_id + 1)))
                posting_list = self._posting_lists[term_id]

                # Append the posting to the posting list. The posting lists
                # must be kept sorted so that we can efficiently traverse and
                # merge them when querying the inverted index. Be paranoid and
                # verify that iterating over documents in the corpus happens
                # in ascending order by document identifiers.
                assert len(posting_list) == 0 or posting_list[-1].document_id < document.document_id
                posting_list.append(Posting(document.document_id, term_frequency))

    def get_terms(self, buffer: str) -> Iterator[str]:
        return (self._normalizer.normalize(t) for t in self._tokenizer.strings(self._normalizer.canonicalize(buffer)))

    def get_postings_iterator(self, term: str) -> Iterator[Posting]:
        # In a serious application a postings list would be stored as a contiguous buffer
        # storing compressed integers, and the iterator would facilitate loading this buffer
        # from somewhere and decompressing the integers.
        term_id = self._dictionary.get_term_id(term)
        return iter([]) if term_id is None else iter(self._posting_lists[term_id])

    def get_document_frequency(self, term: str) -> int:
        # In a serious application we'd store this number explicitly, e.g., as part of the dictionary.
        # That way, we can look up the document frequency without having to access the posting lists
        # themselves. Imagine if the posting lists don't even reside in memory!
        term_id = self._dictionary.get_term_id(term)
        return 0 if term_id is None else len(self._posting_lists[term_id])
