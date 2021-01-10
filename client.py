'''
This module defines the behaviour of a client in your Chat Application
'''
import sys
import getopt
import socket
import random
from threading import Thread
import os
import util


'''
Write your code inside this class. 
In the start() function, you will read user-input and act accordingly.
receive_handler() function is running another thread and you have to listen 
for incoming messages in this function.
'''
checker = 1
class Client:
    '''
    This is the main Client Class. 
    '''
    def __init__(self, username, dest, port, window_size):
        self.server_addr = dest
        self.server_port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(None)
        self.sock.bind(('', random.randint(10000, 40000)))
        self.name = username
        self.window = window_size

    

    def start(self):
        '''
        Main Loop is here
        Start by sending the server a JOIN message.
        Waits for userinput and then process it
        '''
        global checker    
        check = 0
        if check == 0:
            a = util.make_message("join",1, USER_NAME)
            b = util.make_packet("data",0,a)
            self.sock.sendto(b.encode("utf-8"), (self.server_addr, self.server_port))
            check = 1

        
        checker = 1    
        if checker == 1:
            while(checker == 1):
                msg = input()
                if msg == "list":
                    a = util.make_message("request_users_list",2, None)
                    b = util.make_packet("data",0,a)
                    self.sock.sendto(b.encode("utf-8"), (self.server_addr, self.server_port))
                elif msg.split(" ")[0] == "msg":
                    send = msg[4:]
                    a = util.make_message("send_message",4,send) # proper format
                    b = util.make_packet("data",0,a)
                    self.sock.sendto(b.encode("utf-8"), (self.server_addr, self.server_port))
                elif msg.split(" ")[0] == "file":
                    listform = list(msg.split(" "))
                    filetosend = listform[(len(listform)-1)]
                    try:
                        f = open(filetosend,"r")
                        if f.mode == 'r':
                            filedata = f.read()
                            f.close()
                            mess = msg[5:]
                            tosend = mess + " " + filedata
                            a = util.make_message("send_file",4,tosend)
                            b = util.make_packet("data",0,a) 
                            self.sock.sendto(b.encode("utf-8"), (self.server_addr, self.server_port))
                    except:
                        print("File not found")

                elif msg == "quit":
                    print("quitting")
                    a = util.make_message("disconnect",1, USER_NAME)
                    b = util.make_packet("data",0,a)
                    self.sock.sendto(b.encode("utf-8"), (self.server_addr, self.server_port))
                    checker = 0
                    

                elif msg == "help":
                    print("Message- Format: msg <number of users> <user1> <user2> ... <message> " )
                    print("Available Users- Format: list "  )
                    print("File Sharing- Format: file <number of users> <user1> <user2> ... <file name> ")
                    print("Disconnect- Format: quit")
                    print("Help- Format: help")
                else:
                    print("incorrect userinput format")
                    #a = util.make_message("disconnect",1, USER_NAME)
                    #b = util.make_packet("data",0,a)
                    #self.sock.sendto(b.encode("utf-8"), (self.server_addr, self.server_port))
                    #checker = 0

                


                    
                

        #raise NotImplementedError
            

    def receive_handler(self):
        '''
        Waits for a message from server and process it accordingly
        '''
        #raise NotImplementedError
        global checker
        if checker == 1:
            while(checker == 1):
                message, address = self.sock.recvfrom(4096)
                msg_type,seqno,data,checksum = util.parse_packet(message.decode("utf-8"))
                #print(msg_type," ",seqno," ",data," ",checksum)
                #print(data)
                if data[0:15] == "err_server_full":
                    print("Disconnected: Server full")
                    os._exit(1)
                elif data[0:19] == "response_users_list":
                    #print(data)
                    listform = list(data.split(" "))
                    st = ""
                    c = 0
                    for var in listform:
                        if c < 3:
                            c = c +1
                        else:
                            st = st + var + " "
                    st = st[:-1]
                    print("list: " + st)

                elif data[0:15] == "forward_message":
                    needed = data[21:]
                    st = ""
                    count = 1
                    for char in needed:
                        if char == " ":
                            break
                        else:
                            st = st + char
                            count = count + 1
                    print("msg: " + st + ": " + needed[count:])
                elif data[0:12] == "forward_file":
                    print("file: " + list(data.split(" "))[3] + ": " + list(data.split(" "))[4] )
                    listform = list(data.split(" "))
                    msg = ""
                    c = 0
                    for val in listform:
                        if c > 4:
                            msg = msg + val + " "
                        else:
                            c = c + 1
                    msg = msg[:-1]

                    file = listform[4]
                    file = USER_NAME + "_" + file
                    f = open(file,"w")
                    f.write(msg)
                    f.close()



                elif data[0:24] == "err_username_unavailable":
                    print("Disconnected: Username unavailable")
                    checker = 0
                    #os._exit(1)
                elif data[0:19] == "err_unknown_message":
                    print("incorrect userinput format")
                    checker = 0
                    #os._exit(1)
                


# Do not change this part of code
if __name__ == "__main__":
    def helper():
        '''
        This function is just for the sake of our Client module completion
        '''
        print("Client")
        print("-u username | --user=username The username of Client")
        print("-p PORT | --port=PORT The server port, defaults to 15000")
        print("-a ADDRESS | --address=ADDRESS The server ip or hostname, defaults to localhost")
        print("-w WINDOW_SIZE | --window=WINDOW_SIZE The window_size, defaults to 3")
        print("-h | --help Print this help")
    try:
        OPTS, ARGS = getopt.getopt(sys.argv[1:],
                                   "u:p:a:w", ["user=", "port=", "address=","window="])
    except getopt.error:
        helper()
        exit(1)

    PORT = 15000
    DEST = "localhost"
    USER_NAME = None
    WINDOW_SIZE = 3
    for o, a in OPTS:
        if o in ("-u", "--user="):
            USER_NAME = a
        elif o in ("-p", "--port="):
            PORT = int(a)
        elif o in ("-a", "--address="):
            DEST = a
        elif o in ("-w", "--window="):
            WINDOW_SIZE = a

    if USER_NAME is None:
        print("Missing Username.")
        helper()
        exit(1)

    S = Client(USER_NAME, DEST, PORT, WINDOW_SIZE)
    try:
        # Start receiving Messages
        T = Thread(target=S.receive_handler)
        T.daemon = True
        T.start()
        # Start Client
        S.start()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
