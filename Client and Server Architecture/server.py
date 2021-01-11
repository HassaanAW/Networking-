'''
This module defines the behaviour of server in your Chat Application
'''
import sys
import getopt
import socket
import util


class Server:
    '''
    This is the main Server Class. You will to write Server code inside this class.
    '''
    def __init__(self, dest, port, window):
        self.server_addr = dest
        self.server_port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(None)
        self.sock.bind((self.server_addr, self.server_port))
        self.window = window

    def start(self):
        '''
        Main loop.
        continue receiving messages from Clients and processing it
        '''
        #raise NotImplementedError
        users = []
        add = []
        dict = {}
        diction ={}
        while(True):

            check = 0;
            #print("Server is listening to requests ")
            message, address = self.sock.recvfrom(4096)
            #print(address, "said:", message.decode("utf-8"))
            msg_type,seqno,data,checksum = util.parse_packet(message.decode("utf-8"))
            #print(msg_type," ",seqno," ",data," ",checksum)
            #print(data)
            if data[0:4] == "join":
                name = data[7:]
                if len(dict) < util.MAX_NUM_CLIENTS:
                    check = 0
                else:
                    # generate ERR_SERVER_FULL Packet
                    a = util.make_message("err_server_full",2,None)
                    b = util.make_packet("data",0,a)
                    self.sock.sendto(b.encode("utf-8"), address)
                    check = 1
                    print("Disconnected: Server Full")
                
                for val in dict:
                    if val == data[7:]:
                        check = 2
                        print("Disconnected: Username not available")
                        # generate ERR_USERNAME_UNAVAILABLE packet
                        a = util.make_message("err_username_unavailable",2,None)
                        b = util.make_packet("data",0,a)
                        self.sock.sendto(b.encode("utf-8"), address)
                        break

                if check == 0:
                    users.append(name) # maintaining user list
                    add.append(address) # maintaining address list
                    dict.update({name:address}) # maintaining dictionary
                    diction.update({address:name}) # maintaining second dictionary
                    print("join: " + data[7:])
                    #print(dict)
                    #self.sock.sendto("You are connected ".encode("utf-8"), address)
                
            elif data[0:18] == "request_users_list": #dealing with list request
                num = str(len(users))
                users_string = " ".join(sorted(users))
                a = util.make_message("response_users_list",3,num + " " + users_string)
                b = util.make_packet("data",0,a)
                self.sock.sendto(b.encode("utf-8"), address)
                print("request_users_list: " + diction[address])

            elif data[0:12] == "send_message": #dealing with msg requests
                print("msg: " + diction[address])
                space = 0
                count = 0
                for char in data:
                    if space < 2:
                        count = count + 1
                    if char == " ":
                        space = space + 1
                
                required = data[count:]
                num_users = int(required[:1]) # number of receipients
                
                space = 0
                count = 1
                names = "" #string of users
                
                for char in required[2:]:
                    if char == " ":
                        space = space + 1
                    if space < num_users:
                        names = names + char
                        count = count + 1

                msg = required[count+2:] # text message

                listform = list(set(names.split(" ")))

                for var in listform:
                    if var not in users:
                        print("msg: " + diction[address] + " to non-existent user " + var)
                        listform.remove(var)

                tosend = "1" + " " + diction[address] + " " + msg
                for var in listform:
                    a = util.make_message("forward_message", 4, tosend)
                    b = util.make_packet("data", 0, a)
                    self.sock.sendto(b.encode("utf-8"), dict[var])

            elif data[0:10] == "disconnect":
                print("disconnected: " + diction[address])
                to_remove = diction[address]
                users.remove(to_remove)
                add.remove(address)
                del dict[to_remove]
                del diction[address]

            elif data[0:9] == "send_file":
                print("file: " + diction[address])
                listform = list(data.split(" "))
                del listform[0]
                del listform[0]
                num_users = int(listform[0])
                del listform[0]

                c = 0
                msg = ""
                send_list = []
                for val in listform:
                    if c == num_users:
                        filename = val
                    if c < num_users:
                        send_list.append(val)
                    if c < num_users + 1:
                        c = c + 1
                    else:
                        msg = msg + val + " "
                msg = msg[:-1]

                send_list = list(set(send_list))
                
                for var in send_list:
                    if var not in users:
                        print("file: " + diction[address] + " to non-existent user " + var)
                        send_list.remove(var)

                tosend = "1" + " " + diction[address] + " " + filename + " " + msg
                for var in send_list:
                    a = util.make_message("forward_file", 4, tosend)
                    b = util.make_packet("data", 0, a)
                    self.sock.sendto(b.encode("utf-8"), dict[var])


            else:
                print("disconnected: " + diction[address] )
                a = util.make_message("err_unknown_message",2, None)
                b = util.make_packet("data",0,a)
                self.sock.sendto(b.encode("utf-8"), address)
                to_remove = diction[address]
                users.remove(to_remove)
                add.remove(address)
                del dict[to_remove]
                del diction[address]

# Do not change this part of code

if __name__ == "__main__":
    def helper():
        '''
        This function is just for the sake of our module completion
        '''
        print("Server")
        print("-p PORT | --port=PORT The server port, defaults to 15000")
        print("-a ADDRESS | --address=ADDRESS The server ip or hostname, defaults to localhost")
        print("-w WINDOW | --window=WINDOW The window size, default is 3")
        print("-h | --help Print this help")

    try:
        OPTS, ARGS = getopt.getopt(sys.argv[1:],
                                   "p:a:w", ["port=", "address=","window="])
    except getopt.GetoptError:
        helper()
        exit()

    PORT = 15000
    DEST = "localhost"
    WINDOW = 3

    for o, a in OPTS:
        if o in ("-p", "--port="):
            PORT = int(a)
        elif o in ("-a", "--address="):
            DEST = a
        elif o in ("-w", "--window="):
            WINDOW = a

    SERVER = Server(DEST, PORT,WINDOW)
    try:
        SERVER.start()
    except (KeyboardInterrupt, SystemExit):
        exit()
