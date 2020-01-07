import multiprocessing
import socket
import pandas as pd
import sys
import time
import gzip
import shutil
import zlib, base64
from random import randint
import os
from time import sleep
import cv2
import numpy as np
import pickle

buffer_size = 1048576
process_num = None

def handle(connection, i, data, num=0):
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("process")
    try:
        logger.debug("Connected %r at ", connection)
        if process_num == 10 :
            if type(data) == type(b''):
                connection.send(str(len(data)).encode())
                ready = connection.recv(128)
                connection.sendall(data)
            else:
                connection.sendall(data.encode())
        elif process_num == 1 :
            for i in range(10) :
                print("length : ", len(data[i]))
                
                connection.send(str(len(data[i])).encode())
                
                ready = connection.recv(128)
                print(ready.decode())
                
                connection.send(bytes(data[i]))
                print("send ", i, " finish")
                eof = connection.recv(128)
                print(eof.decode())
                print("receive EOF")
            
    finally:
        logger.debug("Closing socket")

class Server(object):
    def __init__(self, hostname, port, process_num):
        import logging
        print(port, process_num)
        self.logger = logging.getLogger("server")
        self.hostname = hostname
        self.port = port
        self.jobs = []
        self.process_num = process_num

    def start(self):
        self.logger.debug("listening")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.hostname, self.port))
        self.socket.listen(10)
        conn, address = self.socket.accept()
        data = conn.recv(buffer_size)
        if data == "":
            logger.debug("Socket closed remotely")
        
        process_num = self.process_num
        
        files = os.listdir(input_file)
        image_files = []
        
        for i in files:
            if 'jpg' in i.split('.')[-1].lower() :
                image_files.append(i)
            
        if len(image_files) < process_num:
            process_num = len(image_files)

        ports = [str(9000 + i) for i in range(process_num)]
        conn.send(str(process_num).encode("UTF-8"))

        print("buffer_size :", buffer_size, "process_num :", process_num)
        
        conn.close()
        socks = []
        newconn = []
        newaddress = []
        for i in range(len(ports)):
            print(ports[i])
            socks.append(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            socks[i].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            socks[i].bind((self.hostname, int(ports[i])))
            self.logger.debug("bound")
            socks[i].listen(10)
            self.logger.debug("listening")
            
        for i in range(len(ports)) :
            subconn, subaddress = socks[i].accept()
            newconn.append(subconn)
            
        if process_num == 10 :
            for i in range(len(ports)) :
                image_files[i] = open(input_file+'/'+image_files[i], 'rb')
                image_files[i] = image_files[i].read()
        elif process_num == 1 :
            single_files = []
            for i in range(len(image_files)) :
                image_files[i] = open(input_file+'/'+image_files[i], 'rb')
                image_files[i] = image_files[i].read()
                single_files.append(image_files[i])
                
        jobs = []
        for i in range(process_num):
            process = None
            if process_num == 10 :
                process = multiprocessing.Process(target=handle, args=(newconn[i], i, image_files[i]))
            elif process_num == 1 :
                process = multiprocessing.Process(target=handle, args=(newconn[i], i, single_files))
            jobs.append(process)
            process.start()
            #self.logger.debug("Started process %r", process)
        self.jobs = jobs
        conn.close()
        for i in socks:
            i.close()
        self.socket.close()


if __name__ == "__main__":
    import logging

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("process_num", type=int)
    parser.add_argument("port", type=int)
    parser.add_argument("input", type=str)
    args = parser.parse_args()
    port, process_num = args.port, args.process_num
    input_file = args.input
        
    print(process_num, port)

    logging.basicConfig(level=logging.DEBUG)
    server = Server("0.0.0.0", port, process_num)
    s = time.time()
    try:
        logging.info("Listening")
        server.start()
    except:
        logging.exception("Unexpected exception")
    finally:
        #logging.info("Shutting down")
        for process in server.jobs:
            #logging.info("Shutting down process %r", process)
            #process.terminate()
            process.join()
    e = time.time()
    print("Communication", e-s)
    logging.info("All done")
    
