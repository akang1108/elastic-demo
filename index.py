from dataclasses import dataclass
from es import *
import requests


@dataclass
class Riddle:
    question: str
    answer: str


def main():
    riddles: [Riddle] = []
    with open("data/riddles.txt") as file_in:
        for line in file_in:
            parts = line.split("&")
            riddles.append(Riddle(parts[0].strip(), parts[1].strip()))

    es = ES("https://10.192.211.178:9200", "elastic", "password")
    print(es.status())

    index(es, riddles)


def index(es: ES, riddles: [Riddle]):
    es.delete_index("riddles")

    # create index
    es.send_request("PUT", "/riddles", body={
        "settings": {
            "index": {
                "number_of_shards": 3,
                "number_of_replicas": 2
            }
        }
    })

    # add docs to index
    for riddle in riddles:
        es.index("riddles", {
            "question": riddle.question,
            "answer": riddle.answer
        })





if __name__ == "__main__":
    main()
