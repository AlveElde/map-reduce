#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
from abc import ABC, abstractmethod
from typing import List, Tuple


class Tokenizer(ABC):
    """
    Simple abstract base class for tokenizers, with some default implementations.
    """

    @abstractmethod
    def ranges(self, buffer: str) -> List[Tuple[int, int]]:
        """
        Returns the positional range pairs that indicate where in the buffer the
        tokens begin and end.
        """
        pass

    def strings(self, buffer: str) -> List[str]:
        """
        Returns the strings that make up the tokens in the given buffer.
        """
        return [buffer[r[0]:r[1]] for r in self.ranges(buffer)]

    def tokens(self, buffer: str) -> List[Tuple[str, Tuple[int, int]]]:
        """
        Returns the (string, range) pairs that make up the tokens in the given buffer.
        """
        return [(buffer[r[0]:r[1]], r) for r in self.ranges(buffer)]


class BrainDeadTokenizer(Tokenizer):
    """
    A dead simple tokenizer for testing purposes. A real tokenizer
    wouldn't be implemented this way. Kids, don't do this at home.
    """

    _pattern = re.compile(r"(\w+)", re.UNICODE | re.MULTILINE | re.DOTALL)

    def __init__(self):
        pass

    def ranges(self, buffer: str) -> List[Tuple[int, int]]:
        return [(m.start(), m.end()) for m in self._pattern.finditer(buffer)]


class ShingleGenerator(Tokenizer):
    """
    Tokenizes a buffer into overlapping shingles having a specified width.
    """

    def __init__(self, width: int):
        assert width > 0
        self._width = width

    def ranges(self, buffer: str) -> List[Tuple[int, int]]:
        """
        Locates where the shingles begin and end. If the buffer is shorter than the shingle width
        then this yields a single shorter-than-usual shingle.

        The current implementation is simplistic and not whitespace- or punctuation-aware,
        and doesn't treat the beginning or end of the buffer in a special way.
        """
        if buffer == "":
            return []
        elif len(buffer) <= self._width:
            return [(0, len(buffer))]
        else:
            return [(i, i + self._width) for i in range(0, len(buffer) - self._width + 1)]
