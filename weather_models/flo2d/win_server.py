import os
import sys
from builtins import print
from http.server import BaseHTTPRequestHandler, HTTPServer
from flo2d_10m_server import start_flo2d_server, stop_flo2d_server

HOST_ADDRESS = '10.138.0.18'
HOST_PORT = 8080


class StoreHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        print('Handle GET request...')
        if self.path.startswith('/start-flo2d-server1'):
            print('StoreHandler|start-flo2d-server1')
            response1 = start_flo2d_server(HOST_ADDRESS, 8091)
            print('StoreHandler|start-flo2d-server|response1 : ', response1)

        if self.path.startswith('/start-flo2d-server2'):
            print('StoreHandler|start-flo2d-server2')
            response2 = start_flo2d_server(HOST_ADDRESS, 8092)
            print('StoreHandler|start-flo2d-server|response2 : ', response2)

        if self.path.startswith('/start-flo2d-server3'):
            print('StoreHandler|start-flo2d-server3')
            response3 = start_flo2d_server(HOST_ADDRESS, 8093)
            print('StoreHandler|start-flo2d-server|response3 : ', response3)

        if self.path.startswith('/stop-flo2d-server4'):
            print('StoreHandler|stop-flo2d-server4')
            response4 = start_flo2d_server(HOST_ADDRESS, 8094)
            print('StoreHandler|start-flo2d-server|response4 : ', response4)


if __name__ == '__main__':
    try:
        print('starting win server...')
        arguments = len(sys.argv) - 1
        if arguments > 0:
            host_address = sys.argv[1]
            host_port = int(sys.argv[2])
        else:
            host_address = HOST_ADDRESS
            host_port = HOST_PORT
        print('starting win server on host {} and port {} '.format(host_address, host_port))
        server_address = (host_address, host_port)
        httpd = HTTPServer(server_address, StoreHandler)
        print('win server running on host {} and port {} ...'.format(host_address, host_port))
        httpd.serve_forever()
    except Exception as e:
        print('Exception : ', str(e))

