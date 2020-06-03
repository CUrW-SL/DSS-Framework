import os
import sys
from builtins import print
from http.server import BaseHTTPRequestHandler, HTTPServer
from flo2d_10m_server import start_flo2d_server, stop_flo2d_server, boot_up_flo2d_server
import json

HOST_ADDRESS = '10.138.0.18'
HOST_PORT = 8080


class StoreHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        print('Handle GET request...')
        if self.path.startswith('/start-flo2d-server1'):
            print('StoreHandler|start-flo2d-server1')
            response1 = boot_up_flo2d_server('flo2d-server1', HOST_ADDRESS, 8091)
            print('StoreHandler|start-flo2d-server|response1 : ', response1)
            reply = json.dumps({'response': 'flo2d-server1-started'})
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/start-flo2d-server2'):
            print('StoreHandler|start-flo2d-server2')
            response2 = boot_up_flo2d_server('flo2d-server2', HOST_ADDRESS, 8092)
            print('StoreHandler|start-flo2d-server|response2 : ', response2)
            reply = json.dumps({'response': 'flo2d-server2-started'})
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/start-flo2d-server3'):
            print('StoreHandler|start-flo2d-server3')
            response3 = boot_up_flo2d_server('flo2d-server3', HOST_ADDRESS, 8093)
            print('StoreHandler|start-flo2d-server|response3 : ', response3)
            reply = json.dumps({'response': 'flo2d-server3-started'})
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/stop-flo2d-server4'):
            print('StoreHandler|stop-flo2d-server4')
            response4 = boot_up_flo2d_server('flo2d-server4', HOST_ADDRESS, 8094)
            print('StoreHandler|start-flo2d-server|response4 : ', response4)
            reply = json.dumps({'response': 'flo2d-server4-started'})
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))


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

