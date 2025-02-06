"""
Module for preparing inverted indexes based on uploaded documents
"""

import sys
from argparse import ArgumentParser, ArgumentTypeError, FileType
from io import TextIOWrapper
from typing import Dict, List
import re
import json

DEFAULT_PATH_TO_STORE_INVERTED_INDEX = "inverted.index"                               # esli v CLI zabyli ukazat --output .... to avto sohraniaet v fail inverted.index


class EncodedFileType(FileType):
    """File encoder"""

    def __call__(self, string):
        # the special argument "-" means sys.std{in,out}
        if string == "-":
            if "r" in self._mode:
                stdin = TextIOWrapper(sys.stdin.buffer, encoding=self._encoding)
                return stdin
            if "w" in self._mode:
                stdout = TextIOWrapper(sys.stdout.buffer, encoding=self._encoding)
                return stdout
            msg = 'argument "-" with mode %r' % self._mode
            raise ValueError(msg)

        # all other arguments are used as file names
        try:
            return open(string, self._mode, self._bufsize, self._encoding, self._errors)
        except OSError as exception:
            args = {"filename": string, "error": exception}
            message = "can't open '%(filename)s': %(error)s"
            raise ArgumentTypeError(message % args)

    def print_encoder(self):
        """printer of encoder"""
        print(self._encoding)


class InvertedIndex:
    """
    This module is necessary to extract inverted indexes from documents.
    """

    def __init__(self, words_ids: Dict[str, List[int]]):
        self.words_ids = words_ids

    def query(self, words: List[str]) -> List[int]:
        """Return the list of relevant documents for the given query"""
        nested_list = []
        for word in words:
            if word in self.words_ids:
                nested_list.append(self.words_ids[word])

        unique_elements = set()
        for sublist in nested_list:
            for item in sublist:
                unique_elements.add(item)

        query_output = list(unique_elements)

        return query_output

    def dump(self, filepath: str) -> None:                                                #(8_idx) sohranenije obratnogo index v JSON file
        """
        Allow us to write inverted indexes documents to temporary directory or local storage
        :param filepath: path to file with documents
        :return: None
        """
        with open(filepath, "w", encoding="utf-8") as json_file:                          # otkryvaet fail v rezhime zapisi
            json.dump(self.words_ids, json_file, ensure_ascii=False, indent=4)            # json.dump zapisyvaet objket v JSON format ... self.words_ids eto obratnyj index. Vse obratnyj index dict gotovo

        return None

    @classmethod
    def load(cls, filepath: str):
        """
        Allow us to upload inverted indexes from either temporary directory or local storage
        :param filepath: path to file with documents
        :return: InvertedIndex
        """
        with open(filepath, "r", encoding="utf-8") as json_file:
            words_ids = json.load(json_file)

        return cls(words_ids)


def load_documents(filepath: str) -> Dict[int, str]:                                   # (6_dict) zagruzhaet dannyje iz faila dataset
    """
    Allow us to upload documents from either tempopary directory or local storage
    :param filepath: path to file with documents
    :return: Dict[int, str]
    """

    documents = {}                                                                    # inicializiruju pustoj slovar

    #pattern = r"(\d+)\t(.+)"                                                          # \d = 0-9, + - odin ili bolee simvolov, . - liuboj simvol, kromesimvola novoj stroki
    with open(filepath, 'r', encoding = 'utf-8') as text:
        for line in text:
            doc_id, content = line.lower().split("\t", 1)                             # razdeliaju po tabuliacij na dve chasti
            doc_id = int(doc_id)
            documents[doc_id] = content                                     # dobaliaju v slovar int:str

    return documents                                                                    # vozvrashaet dict {int:str}, no esce ne indeksnyj

def build_inverted_index(documents: Dict[int, str]) -> InvertedIndex:                  #(6_idx)
    """
    Builder of inverted indexes based on documents
    :param documents: dict with documents
    :return: InvertedIndex class
    """
    # nado iz documents {int:str} sozdat words_ids {str, List[int]]} dlja podachi v class InvertedIndex
    words_ids = {}

    for key, value in documents.items():
        key, value = value, key
        for word in key.split(' '):
            if word not in words_ids:
                words_ids[word] = set()                                               # ispolzuju mnozhestvo dlja hranenije bez duplikatov
            words_ids[word].add(value)

    for word in words_ids:                                                            # preobrazuju nazad value v list
        words_ids[word] = list(words_ids[word])

    inverted_index = InvertedIndex(words_ids)                                         # sozdaju objekt klassa InvertedIndex()

    return inverted_index                                                             # vozvrashaet objekt klassa gotovyj index (perevernuli documents)


def callback_build(arguments) -> None:                                                #(4idx) eto obertka, peredaet parametry iz objekta arguments v def process_build
    """process build runner"""
    return process_build(arguments.dataset, arguments.output)


def process_build(dataset, output) -> None:                                            # iz objekta arguments izvlekajutsja arguments.dataset i arguments.output i peredajutsja siuda
    """
    Function is responsible for running of a pipeline to load documents,
    build and save inverted index.
    :param arguments: key/value pairs of arguments from 'build' subparser
    :return: None
    """
    documents: Dict[int, str] = load_documents(dataset)                                #(5_dict)
    inverted_index = build_inverted_index(documents)                                   #(5_idx)
    inverted_index.dump(output)                                                        #(7_idx) ispolzuem metod dump dlja sohranenija index dict v fail


def callback_query(arguments) -> None:                                                 #(4_que)
    """ "callback query runner"""
    process_query(arguments.query, arguments.index)


def process_query(queries, index) -> None:                                              #(5_que)
    """
    Function is responsible for loading inverted indexes
    and printing document indexes for key words from arguments.query
    :param arguments: key/value pairs of arguments from 'query' subparser
    :return: None
    """
    inverted_index = InvertedIndex.load(index)
    for query in queries:
        print(f"Word: '{query}' in document with index:")
        if isinstance(query, str):
            query = query.strip().split()

        doc_indexes = ",".join(str(value) for value in inverted_index.query(query))
        print(doc_indexes)
        #print(f"Index: {index.words_ids}")
        #print(f"Queries: {words}")


def setup_subparsers(parser) -> None:                                                    # (2) nastraivaju podkomandy(buil i query)
    """
    Initial subparsers with arguments.
    :param parser: Instance of ArgumentParser
    """
    subparser = parser.add_subparsers(dest="command")
    build_parser = subparser.add_parser(                                                 # dobavliaju podkomandu build
        "build",
        help="this parser is need to load, build"
        " and save inverted index bases on documents",
    )
    build_parser.add_argument(
        "-d",
        "--dataset",
        required=True,
        help="You should specify path to file with documents. ",
    )
    build_parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_PATH_TO_STORE_INVERTED_INDEX,                                      #
        help="You should specify path to save inverted index. "
        "The default: %(default)s",
    )
    build_parser.set_defaults(callback=callback_build)                                     # ustanavlivaem obrabotchik callback, kotoryj budet vyzvan. Sviazyvaetsja s funkcijei callback_build(arguments)

    query_parser = subparser.add_parser(                                                   # dobavliaju podkomandu query
        "query", help="This parser is need to load and apply inverted index"
    )
    query_parser.add_argument(
        "--index",
        default=DEFAULT_PATH_TO_STORE_INVERTED_INDEX,
        help="specify the path where inverted indexes are. " "The default: %(default)s",
    )
    query_file_group = query_parser.add_mutually_exclusive_group(required=True)
    query_file_group.add_argument(
        "-q",
        "--query",
        dest="query",
        action="append",
        nargs="+",
        help="you can specify a sequence of queries to process them overall",
    )
    query_file_group.add_argument(
        "--query_from_file",
        dest="query",
        type=EncodedFileType("r", encoding="utf-8"),
        #default=TextIOWrapper(sys.stdin.buffer, encoding='utf-8'),
        help="query file to get queries for inverted index",
    )
    query_parser.set_defaults(callback=callback_query)                                    # ustanavlivaem obrabotchik callback, kotoryj budet vyzvan


def main():
    """
    Starter of the pipeline
    """
    parser = ArgumentParser(
        description="Inverted Index CLI is need to load, build,"
        "process query inverted index"
    )                                                                                   # (1) sozdanije CLI, dlja dobavki i obrabotki argumentov cerez komandnuju stroku
    setup_subparsers(parser)                                                            # vyzyvaju funkciju dlja nastroiki subcommands (build, query)
    arguments = parser.parse_args()                                                     # parse_args() raspoznaet argumenty peredannyje cherez komandnuju stroku. Vozvrashaet objekt arguments = {"dataset": "data.txt","output": "index.json","callback": process_build}
    arguments.callback(arguments)                                                       # vyzyvaet nuzhnuju podkomandu funkcij, naprimer: callback_build(arguments)
                                                                                        # (3) v itoge zdes poluchaem logiku Building index from data.txt, saving to index.json

#zapuskaet proverku script zapushen napriamuju li, esli napriamuju to __name__ vsegda imeet znachenije __main__
if __name__ == "__main__":
    main()


# Rabota s komandnoj strokoj s Arparse (uproshaet rabotu)

#vyzov build (sozdat tablicu indeksirovanija slov)
#python final_task.py build --dataset wikipedia_sample --output inverted.index

#vyzov query
# po slovam (nahodim indeksy dokumentov v lotoryh prisutstvujut slova:(a, book))
# python final_task.py query --index inverted.index --query a book
# po failu (v etom faile dolzhny byt slova dlja poiska indeksov dokumentov)
# python final_task.py query --index inverted.index --query_from_file simple_queries.txt







# * Rabota s kodom, zapisyvaja kod v sam fail (poka beleberda)

#if arguments.command == "build":
    #process_build(arguments)
#lif arguments.command == "query":
    #process_query(arguments)

#global_filepath = "wikipedia_sample"
#documents = load_documents(global_filepath)
#inverted_index = build_inverted_index(documents)

#inverted_index.dump("inverted_index.json")

#loaded_inverted_index = InvertedIndex.load("inverted_index.json")

#print(loaded_inverted_index.words_ids)

#words = ['a', 'is', 'animalia']
#print(inverted_index.query(words))






