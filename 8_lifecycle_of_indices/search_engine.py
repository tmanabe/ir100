#!/usr/bin/env python3

from argparse import ArgumentParser
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs
from urllib.parse import urlparse

import heapq
import json


class Segment:
    def __init__(self, products):
        self.inverted_index_title, self.info_title, self.liveness_id = {}, {}, set()
        for product in sorted(products, key=lambda product: product['product_id']):
            product_id, product_title = product['product_id'], product['product_title']
            # Ref: 2.0
            counter = {}
            for word in product_title.split():
                if word in counter:
                    counter[word] += 1
                else:
                    counter[word] = 1
            for word, count in counter.items():
                if word in self.inverted_index_title:
                    self.inverted_index_title[word].append((product_id, count))
                else:
                    self.inverted_index_title[word] = [(product_id, count)]
            self.info_title[product_id] = product_title
            self.liveness_id.add(product_id)

    def select_iterator(self, word):
        for entry in self.inverted_index_title.get(word, []):
            if entry[0] in self.liveness_id:
                yield entry

# Ref: 2.2
class PriorityQueue:
    def __init__(self, size):
        assert 0 < size
        self.body = []
        self.size = size

    def peek(self):
        return self.body[0]

    def push(self, item):
        if len(self.body) < self.size:
            heapq.heappush(self.body, item)
        else:
            if self.peek() < item:
                heapq.heappushpop(self.body, item)

    def pop(self):
        return heapq.heappop(self.body)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass


class SearchEngine(BaseHTTPRequestHandler):
    @classmethod
    def run(cls, host, port):
        cls.segments = []
        ThreadedHTTPServer((host, port), cls).serve_forever()

    def do_POST(self):
        response, result, contents = 200, {}, self.rfile.read(int(self.headers['content-length'])).decode('utf-8')

        if self.path.startswith('/update'):
            SearchEngine.segments.append(Segment(json.loads(contents)))
            for old_segment in SearchEngine.segments[:-1]:
                old_segment.liveness_id -= SearchEngine.segments[-1].liveness_id
            result['success'] = 'updated {0} products'.format(len(SearchEngine.segments[-1].info_title))

        else:
            response, result['error'] = 404, 'unknown POST endpoint'

        self.send(response, result)

    def do_GET(self):
        response, result, parameters = 200, {}, parse_qs(urlparse(self.path).query)

        if self.path.startswith('/delete'):
            product_ids = set(parameters['product_id'])
            for segment in SearchEngine.segments:
                segment.liveness_id -= product_ids
            result['success'] = 'deleted {0} products if exist'.format(len(product_ids))

        elif self.path.startswith('/select'):
            priority_queue, ranking = PriorityQueue(10), []
            assert 1 == len(parameters['query'])
            for segment_index, segment in enumerate(SearchEngine.segments):
                for product_id, tf in segment.select_iterator(parameters['query'][0]):
                    priority_queue.push((tf, product_id, segment_index))
            while 0 < len(priority_queue.body):
                priority, product_id, segment_index = priority_queue.pop()
                ranking.append({
                    '_priority': priority,
                    'product_id': product_id,
                    'product_title': SearchEngine.segments[segment_index].info_title[product_id],
                })
            result['success'] = ranking

        else:
            response, result['error'] = 404, 'unknown GET endpoint'

        self.send(response, result)

    def send(self, response, result):
        self.send_response(response)
        self.send_header('Content-Type', 'text/json')
        self.end_headers()
        self.wfile.write(json.dumps(result, indent=4).encode('utf-8'))
        self.wfile.write('\n'.encode('utf-8'))


if __name__ == '__main__':
    argument_parser = ArgumentParser(description='runs a search engine')
    argument_parser.add_argument('--host', default='127.0.0.1', help='host name', metavar='str', type=str)
    argument_parser.add_argument('--port', default=8080, help='port number', metavar='int', type=int)
    arg_dict = argument_parser.parse_args()
    SearchEngine.run(arg_dict.host, arg_dict.port)
