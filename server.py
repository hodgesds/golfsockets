#!/usr/bin/env python
# coding:utf-8

import sys, os, re, time
import logging
import gevent.server, gevent.socket
import redis
from threading import Thread
# websocket crap
from geventwebsocket import WebSocketServer, WebSocketApplication, Resource

from gevent import monkey
monkey.patch_all() # I hate this pattern of doing things...

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging

class SocketApplication(WebSocketApplication):
    def __init__(self, *args, **kwargs):
        WebSocketApplication.__init__(self, *args, **kwargs)
        self.red = redis.client.StrictRedis()
        self.sub = self.red.pubsub()
        self.sub.subscribe('master')

    def on_open(self):
        logger.info("WebSocket connection opened...")

    def on_message(self, message):
        logger.info('Subscribing to master')
        for item in self.sub.listen():
            if item['type'] == 'message':
                self.ws.send(str(item.get('data')))

    def on_close(self, reason):
        print reason


def resolve_netloc(netloc, defaultport=80):
    if netloc.rfind(':') > netloc.rfind(']'):
        host, _, port = netloc.rpartition(':')
        port = int(port)
    else:
        host = netloc
        port = defaultport
    if host[0] == '[':
        host = host.strip('[]')
    return host, port

class ForwardHandler(object):
    red = redis.client.StrictRedis()
    def __init__(self, remote_address, client_socket, address):
        logger.info('client %r connected, try to connect remote %r', address, remote_address)
        remote_socket  = gevent.socket.create_connection(remote_address)
        logger.info('connect remote %r ok, begin forward', remote_address)
        gevent.spawn(self.io_copy, client_socket, remote_socket).start()
        gevent.spawn(self.io_copy, remote_socket, client_socket).start()

    def io_copy(self, sock1, sock2, timeout=None, bufsize=4096):
        try:
            if timeout:
                sock1.settimeout(timeout)
            while 1:
                data = sock1.recv(bufsize)
                if not data:
                    break
                try:
                    self.red.publish('master', str(data.encode('ascii')))
                except:
                    logger.error('Error publishing data on redis')
                sock2.send(data)
        except Exception:
            logger.exception('io_copy exception')
        finally:
            logger.info('end forward, closed')
            try:
                sock1.close()
            except:
                pass
            try:
                sock2.close()
            except:
                pass


class ForwardServer(gevent.server.StreamServer):

    def __init__(self, remote_address, *args, **kwargs):
        self.remote_address = remote_address
        super(ForwardServer, self).__init__(*args, **kwargs)

    def handle(self, client_socket, address):
        gevent.spawn(ForwardHandler, self.remote_address, client_socket, address)


if __name__=='__main__':
    if len(sys.argv) == 1:
        print 'usage: portforward.py server_address remote_address websocket_port'
        print 'example: portforward.py 0.0.0.0:80 127.0.0.1:8080 8000'
        sys.exit(0)

    listener = resolve_netloc(sys.argv[1], 80)
    remote_address = resolve_netloc(sys.argv[2], 80)
    if len(sys.argv) > 3:
        try:
            websocket_port = int(sys.argv[3])
        except ValueError:
            sys.exit(0)
    else:
        websocket_port = 8001
    # run websocket in separate thread
    def socket_server(socket_port):
        print 'Starting websocket server...'
        WebSocketServer(
            ('', socket_port),
            Resource({'/': SocketApplication})
        ).serve_forever()

    socket_thread = Thread(target=socket_server, args=(websocket_port,))
    socket_thread.start()

    server = ForwardServer(remote_address, listener)
    print 'Serving Socket on', listener[0], 'port', listener[1], '...'
    server.serve_forever()
