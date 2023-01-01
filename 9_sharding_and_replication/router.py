#!/usr/bin/env python3

from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from hashlib import md5
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from requests import get
from requests import post
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs
from urllib.parse import urlparse

import json


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass


class Router(BaseHTTPRequestHandler):
    @classmethod
    def run(cls, arg_dict):
        cls.arg_dict = arg_dict
        ThreadedHTTPServer((arg_dict.host, arg_dict.port), cls).serve_forever()

    def do_POST(self):
        response, result, contents = 200, {}, self.rfile.read(int(self.headers['content-length'])).decode('utf-8')

        if self.path.startswith('/update'):
            sharded_contents = []
            for _ in range(Router.arg_dict.shards):
                sharded_contents.append([])
            for product in json.loads(contents):
                product_hash = int(md5(product['product_id'].encode('utf-8')).hexdigest(),  16)
                sharded_contents[product_hash % Router.arg_dict.shards].append(product)

            port = self.arg_dict.port
            with ThreadPoolExecutor(max_workers=self.arg_dict.shards * self.arg_dict.replicas) as tpe:
                def fn(url, contents):
                    return {url: json.loads(post(url, data=json.dumps(contents)).text)}

                fs = []
                for contents in sharded_contents:
                    for _ in range(self.arg_dict.replicas):
                        port += 1
                        url = 'http://{0}:{1}/update'.format(self.arg_dict.host, port)
                        fs.append(tpe.submit(fn, url=url, contents=contents))
                result['success'] = {}
                for f in as_completed(fs):
                    result['success'].update(f.result())

        else:
            response, result['error'] = 404, 'unknown POST endpoint'

        self.send(response, result)

    def do_GET(self):
        response, result, parameters = 200, {}, parse_qs(urlparse(self.path).query)

        if self.path.startswith('/select'):
            pass

        if self.path.startswith('/truncate'):
            port = self.arg_dict.port
            result['success'] = {}
            for _ in range(self.arg_dict.shards):
                for _ in range(self.arg_dict.replicas):
                    port += 1
                    url = 'http://{0}:{1}/truncate'.format(self.arg_dict.host, port)
                    result['success'][url] = json.loads(get(url).text)

        else:
            response, result['error'] = 404, 'unknown GET endpoint'

        self.send(response, result)

    def send(self, response, result):
        self.send_response(response)
        self.send_header('Content-Type', 'text/json')
        self.end_headers()
        self.wfile.write(json.dumps(result, indent=4).encode('utf-8'))


if __name__ == '__main__':
    argument_parser = ArgumentParser(description='runs a router')
    argument_parser.add_argument('--host', default='127.0.0.1', help='host name', metavar='str', type=str)
    argument_parser.add_argument('--port', default=8080, help='port number', metavar='int', type=int)
    argument_parser.add_argument('--replicas', default=1, help='number of replicas', metavar='int', type=int)
    argument_parser.add_argument('--shards', default=2, help='number of shards', metavar='int', type=int)
    Router.run(argument_parser.parse_args())
