#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
import collections.abc
from typing import Dict, Any


class Document(ABC):
    """
    Abstract base class for a document. A document is a simple collection of
    named, typed fields.
    """

    def __getitem__(self, field_name: str) -> Any:
        return self.get_field(field_name, None)

    @property
    def document_id(self) -> int:
        return self.get_document_id()

    @abstractmethod
    def get_document_id(self) -> int:
        """
        Returns the document's unique identifier.
        """
        pass

    @abstractmethod
    def get_field(self, field_name: str, default: Any) -> Any:
        """
        Returns the value of the named field in the document. If the document
        doesn't contain the named field, the provided default field value is
        returned instead.
        """
        pass


class InMemoryDocument(Document):
    """
    A very simple and straightforward in-memory implementation of a document.
    Note that what we index are normalized versions of the raw fields. We keep
    the raw fields here in order to preserve the original presentation.
    """

    def __init__(self, document_id: int, fields: Dict[str, Any]):
        self._document_id = document_id
        assert isinstance(fields, dict)
        self._fields = fields

    def __repr__(self):
        return str({"document_id": self._document_id, "fields": self._fields})

    def get_document_id(self) -> int:
        return self._document_id

    def get_field(self, field_name: str, default: Any) -> Any:
        return self._fields.get(field_name, default)


class Corpus(collections.abc.Iterable):
    """
    Abstract base class representing a corpus we can index and search over,
    i.e., a collection of documents. The class facilitates iterating over
    all documents in the corpus.
    """

    def __len__(self):
        return self.size()

    def __getitem__(self, document_id: int) -> Document:
        return self.get_document(document_id)

    @abstractmethod
    def size(self) -> int:
        """
        Returns the size of the corpus, i.e., the number of documents in the
        document collection.
        """
        pass

    @abstractmethod
    def get_document(self, document_id: int) -> Document:
        """
        Returns the document associated with the given document identifier.
        """
        pass


class InMemoryCorpus(Corpus):
    """
    An in-memory implementation of a document store, suitable only for small
    document collections.

    Document identifiers are assigned on a first-come first-serve basis.
    """

    def __init__(self, filename=None):
        self._documents = []
        if filename:
            if filename.endswith(".txt"):
                self._load_text(filename)
            elif filename.endswith(".xml"):
                self._load_xml(filename)
            elif filename.endswith(".json"):
                self._load_json(filename)
            elif filename.endswith(".csv"):
                self._load_csv(filename)
            else:
                raise IOError("Unsupported extension")

    def __iter__(self):
        return iter(self._documents)

    def size(self) -> int:
        return len(self._documents)

    def get_document(self, document_id: int) -> Document:
        assert 0 <= document_id < len(self._documents)
        return self._documents[document_id]

    def add_document(self, document: Document) -> None:
        """
        Adds the given document to the corpus. Facilitates testing.
        """
        assert document.document_id == len(self._documents)
        self._documents.append(document)

    def _load_text(self, filename):
        """
        Loads documents from the given UTF-8 encoded text file. One document per line,
        tab-separated fields. Empty lines are ignored. The first field gets named "body",
        the second field (optional) gets named "meta". All other fields are currently ignored.
        """
        document_id = 0
        with open(filename, mode="r", encoding="utf-8") as f:
            for line in f:
                anonymous_fields = line.strip().split("\t")
                if len(anonymous_fields) == 1 and not anonymous_fields[0]:
                    continue
                named_fields = {"body": anonymous_fields[0]}
                if len(anonymous_fields) >= 2:
                    named_fields["meta"] = anonymous_fields[1]
                self.add_document(InMemoryDocument(document_id, named_fields))
                document_id += 1

    def _load_xml(self, filename):
        """
        Loads documents from the given XML file. The schema is assumed to be
        simple <doc> nodes. Each <doc> node gets mapped to a single document field
        named "body".
        """
        from xml.dom.minidom import parse

        def __get_text(nodes):
            data = []
            for node in nodes:
                if node.nodeType == node.TEXT_NODE:
                    data.append(node.data)
            return " ".join(data)

        dom = parse(filename)
        document_id = 0
        for body in [__get_text(n.childNodes) for n in dom.getElementsByTagName("doc")]:
            self.add_document(InMemoryDocument(document_id, {"body": body}))
            document_id += 1

    def _load_csv(self, filename):
        """
        Loads documents from the given UTF-8 encoded CSV file. One document per line.
        """
        import csv
        document_id = 0
        with open(filename, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.add_document(InMemoryDocument(document_id, dict(row)))
                document_id += 1

    def _load_json(self, filename):
        """
        Loads documents from the given UTF-8 encoded JSON file. One document per line.
        Lines that do not start with "{" are ignored.
        """
        from json import loads
        document_id = 0
        with open(filename, mode="r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("{"):
                    named_fields = loads(line)
                    self.add_document(InMemoryDocument(document_id, named_fields))
                    document_id += 1
