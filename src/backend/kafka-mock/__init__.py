# WORKING IN PROGRESS

from dataclasses import dataclass
from kafka import KafkaConsumer
import json


@dataclass
class Movie:
    def __init__(self, movie_id: str, title: str):
        self.movie_id = movie_id
        self.title = title


@dataclass
class Comment:
    def __init__(self, comment_id: str, content: str, movie: Movie):
        self.comment_id = comment_id
        self.content = content
        self.movie = movie


consumer = KafkaConsumer('new-comment')
for msg in consumer:
    json_data = json.loads(msg.decode('utf-8'))
    comment_id, content, movie_id, movie_title = json_data['comment_id'], json_data['content'], json_data['movie_id'], \
        json_data['movie_title']
    movie = Movie(movie_id, movie_title)
    comment = Comment(comment_id, content, movie)


def process_comment(comment: Comment):
    print(f'[WIP] data processing for new comment {comment}')
    pass
