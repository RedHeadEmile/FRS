import socket
import sys
import time
from threading import Thread
from typing import List, Dict


class FRSError(Exception):
    pass


class Client:

    sock: socket
    __host: str = ""
    __port: int = 0
    __user: str = ""
    __password: str = ""
    __run: bool = False
    __t1: Thread
    __publish_callback = None
    __responses = {}
    __debug: bool = False

    def __init__(self, host: str = "localhost", port: int = 4514, user: str = "", password: str = "", publish_callback=None, debug: bool = False):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__publish_callback = default_publish_callback if publish_callback is None else publish_callback
        self.__debug = debug

    def start(self):
        if not self.__run:
            self.__run = True
            self.log()
            self.__t1 = Thread(target=self.main_lop)
            self.__t1.setDaemon(True)
            self.__t1.start()

    def send_message(self, message: str, need_rep: bool = False) -> str:
        self.check_start()
        try:
            # Send data
            message = message.replace("\n", "\\n")
            if self.__debug:
                print(sys.stderr, 'sending "%s"' % message)
            if need_rep:
                identifier = str(int(round(time.time() * 1000)))
                self.sock.sendall(str.encode(identifier + " " + message + '\n'))
                t = time.time()
                while identifier not in self.__responses and time.time() - t < 10:
                    time.sleep(0.01)
                if identifier in self.__responses:
                    rep = self.__responses[identifier]
                    del self.__responses[identifier]
                    return rep
                else:
                    raise FRSError("answer for message \"%s\" time out" % message)
            else:
                self.sock.sendall(str.encode("0 " + message + '\n'))
                return ""
        except ConnectionResetError:
            if need_rep:
                self.log()
                raise FRSError("Frs disconnected can't send message with response")
            else:
                self.log(0, message, need_rep)

    def publish(self, channel: str, message: str):
        self.send_message("publish" + " " + channel + " " + message)

    def check_start(self):
        if not self.__run:
            raise FRSError("try action with FRSClient without start")

    def log(self, nb_try: int = 0, *retry_args):
        self.check_start()
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = (self.__host, self.__port)
            if self.__debug:
                print(sys.stderr, 'connecting to %s port %s' % server_address)
            self.sock.connect(server_address)
            if len(retry_args) > 0:
                self.send_message(*retry_args)
        except ConnectionRefusedError:
            if nb_try < 5:
                print(sys.stderr, "connection failed retry")
                self.log(nb_try + 1, retry_args)
            else:
                print(sys.stderr, "connection failed 5 times disconnected")

    def main_lop(self):
        current: str = ""
        while self.__run:
            try:
                data = self.sock.recv(1024)
                if data:
                    current += data.decode("utf-8")
                    if current.endswith('\n'):
                        current, new = "", current[:-1]
                        for line in new.split('\n'):
                            self.on_message_received(line)
            except ConnectionAbortedError:
                if self.__run:
                    self.log()
                    self.main_lop()

    def on_message_received(self, message: str):
        if message:
            if message != "imhere":
                data = message.split(" ")
                if data[0] == "publish":
                    channel = data[1]
                    final_message = message[len(data[0] + data[1]) + 2::].replace('\\n', '\n')
                    self.__publish_callback(channel, final_message)
                elif data[0] == "rep":
                    identifier = data[1]
                    value = message[len(data[0] + data[1]) + 2::].replace('\\n', '\n')
                    self.__responses[identifier] = value

    def set_value(self, key: str, field: str, value: str):
        self.send_message("set %s %s %s" % (Client.check_key(key), Client.check_key(field), value))

    @staticmethod
    def check_key(value):
        return value.replace(' ', '_') if value is not None else "null"

    def get_value(self, key: str, field: str) -> str: 
        return self.send_message("get %s %s" % (Client.check_key(key), Client.check_key(field)), True)
    
    def get_fields(self, key: str) -> List[str]:
        result = self.send_message("getall %s" % Client.check_key(key), True)
        return result[:-1].split(" ")

    def get_values(self, key: str, *fields: str) -> Dict[str, str]:
        back = {}
        for field in fields:
            back[field] = self.get_value(key, field)
        return back
    
    def close(self):
        self.__run = False
        self.sock.close()


# END FRSClient class
def default_publish_callback(channel: str, message: str):
    print("Message received from channel \"" + channel + "\": \"" + message + "\"")
