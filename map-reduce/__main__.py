from .corpus import InMemoryDocument, InMemoryCorpus
from .invertedindex import InMemoryInvertedIndex
from .normalization import BrainDeadNormalizer
from .tokenization import BrainDeadTokenizer



def main():
    corpus = InMemoryCorpus()
    normalizer = BrainDeadNormalizer()
    tokenizer = BrainDeadTokenizer()
    corpus.add_document(InMemoryDocument(0, {"body": "this is a Test"}))
    corpus.add_document(InMemoryDocument(1, {"body": "test TEST prØve"}))
    index = InMemoryInvertedIndex(corpus, ["body"], normalizer, tokenizer)

if __name__ == "__main__":
    main()