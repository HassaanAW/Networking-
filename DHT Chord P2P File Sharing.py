import socket 
import threading
import os
import time
import hashlib
import time
'''
Hassaan Ahmad Waqar 22100137

BackUp Files:
Files should be backed up at the successor. This is because if any node fails abrubtly, its share of files need to be hashed to the successor.
In case we are already backing up files at the successor, we would not need to worry about misplacement of files. When a node is down suddenly, the predecessor
only signals the second successor (successor of killed node) about it, which then puts the backupfiles into the list of self.files

All tests are passing on multiple ports, though I've tested on Windows because my Linux was down.
'''



class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (self.host, self.port)
		self.predecessor = (self.host, self.port)
		self.second = None
		
		self.timer = time.time()
		self.latest_file  = None
		self.file_dictionary = {}
		self.names = []
		self.count = 0
		self.retry = 0

		# additional state variables



	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

	def lookup(self, port): # parameter is port, make key here
		
		key_id = self.hasher(self.host + str(port) ) # key found
		get_successor = self.hasher(self.successor[0]+str(self.successor[1]) ) # gives key of successor
		get_predecessor = self.hasher(self.predecessor[0] +str(self.predecessor[1]) ) # gives key of predecessor

		if (key_id < self.key and self.key < get_predecessor): # base case of two initial nodes
			reply =(self.host, self.port)
			return reply

		elif (get_predecessor < key_id < self.key): # I am the successor
			reply = (self.host, self.port)
			return reply

		elif (key_id < get_predecessor < self.key < get_successor): # pass on to successor
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
			s.connect( (self.successor[0], self.successor[1]) )
			to_send = "Lookup" + "," + str(port)
			s.send(to_send.encode("utf-8"))

			msg = s.recv(1024).decode("utf-8")
			listform = msg.split(',')
			reply = ( listform[1], int(listform[2]) )
			s.close()
			return reply

		elif (self.key < get_successor < key_id): # pass on to the successor
			# create a TCP socket object here 
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
			s.connect( (self.successor[0], self.successor[1]) )
			to_send = "Lookup" + "," + str(port)
			s.send(to_send.encode("utf-8"))

			# wait for reply
			msg = s.recv(1024).decode("utf-8")
			# print("Message", msg)
			listform = msg.split(',')
			reply = ( listform[1], int(listform[2]) )
			s.close()
			return reply
		
		elif (self.key > get_successor and self.key > key_id):
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
			s.connect( (self.successor[0], self.successor[1]) )
			to_send = "Lookup" + "," + str(port)
			s.send(to_send.encode("utf-8"))

			msg = s.recv(1024).decode("utf-8")
			listform = msg.split(',')
			reply = ( listform[1], int(listform[2]) )
			s.close()
			return reply

		else:
			reply = (self.successor[0], self.successor[1])
			return reply

	def Search_Node(self, key_id): # parameter is key
		
		get_successor = self.hasher(self.successor[0]+str(self.successor[1]) ) # gives key of successor
		get_predecessor = self.hasher(self.predecessor[0] +str(self.predecessor[1]) ) # gives key of predecessor

		if (key_id < self.key and self.key < get_predecessor): # base case of two initial nodes
			reply =(self.host, self.port)
			return reply

		elif (get_predecessor < key_id < self.key): # I am the successor
			reply = (self.host, self.port)
			return reply

		elif (key_id < get_predecessor < self.key < get_successor): # pass on to successor
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
			s.connect( (self.successor[0], self.successor[1]) )
			to_send = "Search" + "," + str(key_id)
			s.send(to_send.encode("utf-8"))

			msg = s.recv(1024).decode("utf-8")
			listform = msg.split(',')
			reply = ( listform[1], int(listform[2]) )
			s.close()
			return reply

		elif (self.key < get_successor < key_id): # pass on to the successor
			# create a TCP socket object here 
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
			s.connect( (self.successor[0], self.successor[1]) )
			to_send = "Search" + "," + str(key_id)
			s.send(to_send.encode("utf-8"))

			# wait for reply
			msg = s.recv(1024).decode("utf-8")
			# print("Message", msg)
			listform = msg.split(',')
			reply = ( listform[1], int(listform[2]) )
			s.close()
			return reply

		elif (self.key > get_successor and self.key > key_id):
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
			s.connect( (self.successor[0], self.successor[1]) )
			to_send = "Search" + "," + str(key_id)
			s.send(to_send.encode("utf-8"))

			msg = s.recv(1024).decode("utf-8")
			listform = msg.split(',')
			reply = ( listform[1], int(listform[2]) )
			s.close()
			return reply
			
		else:
			reply = (self.successor[0], self.successor[1])
			return reply


	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		message = client.recv(1024)
		text = message.decode("utf-8")

		listform = text.split(',')
		# print(listform)

		if listform[0] == "Lookup": # This is for Join only- To search for the successor

			if(self.port == self.successor[1] and self.port == self.predecessor[1]): # base case only --------------------------
				#print("in here")
				make_addr = (self.host, int(listform[1]) ) 
				self.successor = make_addr
				self.predecessor = make_addr
				to_send = "base" + "," + self.host + "," + str(self.port)
				client.send(to_send.encode("utf-8"))
				self.Update_Second()
				self.Call()
				# working till here - Base cases handled ---------------------------------------------------------------------
			
			else:
				get_port = int(listform[1])
				reply = self.lookup(get_port)
				addr = "NotBase" + "," + reply[0] + "," + str(reply[1])
				client.send(addr.encode("utf-8"))

		elif listform[0] == "Update_Pred":
			new_pred = int(listform[1])
			old_pred = str(self.predecessor[1] )
			addr = (self.host, new_pred)
			self.predecessor = addr # updated Predecessor and returned previous predecessor
			to_send = "Old_Pred" + "," + old_pred
			client.send(to_send.encode("utf-8"))
			# print(self.port, "MY succ", self.successor)
			# print(self.port, "MY Pred", self.predecessor)

		elif listform[0] == "Update_Succ":
			new_succ = int(listform[1])
			addr = (self.host, new_succ)
			self.successor = addr # updated Successor
			self.Update_Second()
			self.Call()
			# print(self.port, "MY succ", self.successor)
			# print(self.port, "MY Pred", self.predecessor)
		# Join process completes here -----------------------------------------------------------------------------------------
		
		elif listform[0] == "Call":
			self.Update_Second()

		elif listform[0] == "Search":
			get_key = int(listform[1])
			reply = self.Search_Node(get_key)
			addr = "Found" + "," + reply[0] + "," + str(reply[1])
			client.send(addr.encode("utf-8"))

		elif listform[0] == "Incoming":
			file_name = listform[1]
			appended_name = self.host + "_"+ str(self.port) + "/" + file_name
			self.latest_file = appended_name
			file_hash = self.hasher(file_name)
			self.file_dictionary.update({file_hash:file_name})
			#print(self.port, self.file_dictionary)
			self.files.append(file_name)
			#print(appended_name) 

		elif listform[0] == "Leave_Protocol":
			#print(self.port)
			file_name = listform[1]
			#print(file_name)
			appended_name = self.host + "_"+ str(self.port) + "/" + file_name
			self.latest_file = appended_name
			file_hash = self.hasher(file_name)
			self.file_dictionary.update({file_hash:file_name})
			#print(self.port, self.file_dictionary)
			self.files.append(file_name)
			client.send("OK".encode("utf-8"))
			#print(appended_name) 

			message = client.recv(1024)
			#print(self.port, self.latest_file)
			self.recieveFile(client, self.latest_file, 32)

			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
			s.connect(self.successor) #connect to the other successor
			to_send = "Please" + "," + file_name
			s.send( to_send.encode("utf-8"))
			message = s.recv(1024)

			# appended_name = self.host + "_"+ str(self.port) + "/" + file_name
			# self.sendFile(s, appended_name)

			s.close()

			client.send("OK".encode("utf-8"))


			# Backup files

		elif listform[0] == "Please":
			file = listform[1]
			#print(self.port, file)
			self.backUpFiles.append(file)
			client.send("OK".encode("utf-8"))

			# message = client.recv(1024)
			# appended_name = self.host + "_"+ str(self.port) + "/" + file
			# self.recieveFile(client, appended_name, 32)

			#print("Back", self.backUpFiles)

		elif listform[0] == "Sending_backup":
			latest_file = listform[1]
			appended_name = self.host + "_"+ str(self.port) + "/" + file_name
			client.send("OK".encode("utf-8"))

			self.recieveFile(client, appended_name, 32)
			if latest_file not in self.backUpFiles:
				self.backUpFiles.append(latest_file)

		
		elif listform[0] == "Send_File":
			file_name = listform[1]
			file_hash = self.hasher(file_name)
			if file_hash in self.file_dictionary:
				# client.send(file_name.encode("utf-8"))
				appended_name = self.host + "_"+ str(self.port) + "/" + file_name
				self.sendFile(client, appended_name)
			else:
				client.send("None".encode("utf-8"))

		elif listform[0] == "32":
			size = int(listform[0])
			#print(self.port, self.latest_file)
			self.recieveFile(client, self.latest_file, size)

			# Send to successor for back up
		elif listform[0] == "Back":
			# print(self.port)
			# print(self.backUpFiles)
			for files in self.backUpFiles:
				self.files.append(files)
			# print(self.files)


		elif listform[0] == "Transfer":
			get_port = int(listform[1])
			get_hash = self.hasher(self.host + str(get_port) ) # key found
			if len(self.file_dictionary) == 0: # No files
				client.send("None".encode("utf-8"))
			else: # some files exist
				temp_dict = {}
				for key, value in self.file_dictionary.items():
					reply = self.Search_Node(key)
					# if key < get_hash:
					if reply[1] == get_port:
						self.backUpFiles.append(value) #newly added
						temp_dict.update({key:value})
				#print(self.port, temp_dict)
				if len(temp_dict) == 0: # No file to send
					client.send("None".encode("utf-8"))
				else: # files to send
					to_send = "Coming" + ","
					for each, value in temp_dict.items():
						to_send = to_send + value + ","
					client.send(to_send.encode("utf-8"))
					
					for each, value in temp_dict.items():
						appended_name = self.host + "_"+ str(self.port) + "/" + value
						self.sendFile(client, appended_name)
					
					for each, value in temp_dict.items():
						del self.file_dictionary[each]
						self.files.remove(value)
						appended_name = self.host + "_"+ str(self.port) + "/" + value
						#os.remove(appended_name) #commented out

		elif listform[0] == "Leave_Up_Succ":
			get_port = int(listform[1])
			successor = (self.host, get_port)
			self.successor = successor
			self.Update_Second()
			self.Call()

		elif listform[0] == "Give_Succ":
			get_succ = str(self.successor[1])
			client.send(get_succ.encode("utf-8"))

		elif listform[0] == "Leave_Up_Pred":
			get_port = int(listform[1])
			predecessor = (self.host, get_port)
			self.predecessor = predecessor

		elif listform[0] == "Second":
			get_successor = str(self.successor[1])
			client.send(get_successor.encode("utf-8"))
		else:
			pass


	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''
		# print("Newly Added", self.port, self.key)
		# print("Connected to", joiningAddr)
		threading.Thread(target = self.Ping ).start()
		# threading.Thread(target = self.Second ).start()
		if(joiningAddr == ""):
			self.successor = (self.host, self.port)
			self.predecessor = (self.host, self.port)
		else:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
			s.connect(joiningAddr) #connect to the other successor
			to_send = "Lookup" + "," + str(self.port)
			s.send( to_send.encode("utf-8"))

			msg = s.recv(1024)
			reply =  msg.decode("utf-8") 
			s.close()
			listform = reply.split(",")

			if listform[0] == "base":
				self.successor = joiningAddr
				self.predecessor = joiningAddr
				self.Update_Second()
				self.Call()
				# Base case handled ---------------------------------------------------------------

			elif listform[0]  == "NotBase":
				addr = (listform[1], int(listform[2]) )
				self.successor = addr # successor updated
				self.Update_Second()
				self.Call()
				# print(self.port, "Updated Succ", self.successor) # Contact Successor and tell him to update Pred

				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
				s.connect(self.successor) #connect to the other successor
				to_send = "Update_Pred" + "," + str(self.port)
				s.send( to_send.encode("utf-8"))

				msg = s.recv(1024)
				reply =  msg.decode("utf-8") 
				s.close()
				listform = reply.split(",")

				addr = (self.host, int(listform[1]) )
				self.predecessor = addr  # updated Predeccesor. Now connect with Predecessor and Ask it to update Successor
				
				# print(self.port, "Updated Succ", self.successor)
				# print(self.port, "Updated Pred", self.predecessor)
				
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
				s.connect(self.predecessor) #connect to the other predecessor
				to_send = "Update_Succ" + "," + str(self.port)
				s.send( to_send.encode("utf-8"))
				# working till here -------------------------------------------------------------------

				# Ask for file transfer now ----------------------------------------------------------- 
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
				s.connect(self.successor) #connect to the other predecessor
				to_send = "Transfer" + "," + str(self.port)
				s.send( to_send.encode("utf-8"))

				check = True
				local_count = 0
				while(check == True):
					msg = s.recv(1024)
					reply =  msg.decode("utf-8") 
					listform = reply.split(",")
					#print(listform)
					if listform[0] == "None":
						check = False
						s.close()
					else:
						if listform[0] == "Coming":
							listform = listform[:-1]
							for i in range(1, len(listform)):
								self.names.append(listform[i])
								self.files.append(listform[i])
							self.count = len(self.names)

						else:
							file_size = int(listform[0])
							appended_name = self.host + "_"+ str(self.port) + "/" + self.names[local_count]
							self.recieveFile(s, appended_name,file_size)
							local_count = local_count + 1

						if local_count == self.count:
							check = False
							self.count = 0
							self.names.clear()
							s.close()
				# Transfer complete -------------------------------------------------------------------

				# Back Up here on successor

			else:
				pass

	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		#print(fileName)
		get_hash = self.hasher(fileName)
		#print("Hash", get_hash)
		find_node = self.Search_Node(get_hash)
		#print("Find node", find_node) #---------------- Appropriate node correctly found
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
		s.connect(find_node) #connect to the node
		to_send = "Incoming" + "," + fileName
		s.send( to_send.encode("utf-8"))
		s.close()
		time.sleep(0.5)
		
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
		s.connect(find_node)
		self.sendFile(s,fileName)
		s.close()

	def Call(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
		s.connect(self.predecessor) #connect to the node
		to_send = "Call"
		s.send( to_send.encode("utf-8"))
		s.close()
		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''
		get_hash = self.hasher(fileName)
		# print("Hash", get_hash)
		find_node = self.Search_Node(get_hash)
		# print("Find node", find_node) #---------------- Appropriate node correctly found
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
		s.connect(find_node) #connect to the node
		to_send = "Send_File" + "," + fileName
		s.send( to_send.encode("utf-8"))
		
		msg = s.recv(1024)
		reply =  msg.decode("utf-8") 
		listform = reply.split(",")
		#print(listform)
		if listform[0] == "None":
			s.close()
			return None
		else: # recv file now
			file_size = int(listform[0])
			self.recieveFile(s,fileName,file_size)
			s.close()
			return fileName
	
	def BackupFiles(self):

		if len(self.files) != 0:
			for files in self.files:
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
				s.connect(self.successor) #connect to the other successor
				to_send = "Sending_backup" + "," + file
				s.send( to_send.encode("utf-8"))
				appended_name = self.host + "_"+ str(self.port) + "/" + files

				s.recv(1024)

				self.sendFile(s, appended_name)
				s.close()
		else:
			pass


	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''
		#self.Update_Second()
		# print(self.second)
		# print(self.port)
		# print(self.successor)

		if len(self.files) == 0:
			pass
		else:
			for file in self.files:
				# print(file)
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
				s.connect(self.successor) #connect to the other successor
				to_send = "Leave_Protocol" + "," + file
				s.send( to_send.encode("utf-8"))
				#s.close()
				# time.sleep(0.5)
				s.recv(1024).decode("utf-8")

				# s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
				# s.connect(self.successor) #connect to the other predecessor
				appended_name = self.host + "_"+ str(self.port) + "/" + file
				self.sendFile(s, appended_name)

				s.recv(1024).decode("utf-8")
				s.close()

				os.remove(appended_name)
			
			self.files.clear()
			self.file_dictionary.clear()
			# All files sent and states cleared

		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
		s.connect(self.predecessor) #connect to the other predecessor
		to_send = "Leave_Up_Succ" + "," + str(self.successor[1])
		s.send( to_send.encode("utf-8"))
		s.close()

		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
		s.connect(self.successor) #connect to the successor
		to_send = "Leave_Up_Pred" + "," + str(self.predecessor[1])
		s.send( to_send.encode("utf-8"))
		s.close()

		self.stop = True


	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName, fileSize):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		#fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True

	def Update_Second(self):

		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
		s.connect(self.successor) #connect to the other successor
		to_send = "Second"
		s.send( to_send.encode("utf-8"))

		msg = s.recv(1024)
		reply =  msg.decode("utf-8") 
		s.close()
		listform = reply.split(",")
		get_second = int(listform[0])
		make_addr = (self.host, get_second)
		self.second = make_addr


	def Ping(self):
		while(True):
			new_time = time.time()
			if new_time - self.timer >=0.3:
				self.timer = new_time
				try:
					s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
					s.connect(self.successor) #connect to the other successor
					to_send = "Ping" + "," + str(self.port)
					s.send( to_send.encode("utf-8"))
					s.close()
					
				except:
					s.close()
					self.successor = self.second
					#print("New", self.port, self.successor)
					# print(self.successor)
					s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
					s.connect(self.successor) #connect to the other successor
					to_send = "Back"
					s.send( to_send.encode("utf-8"))
					s.close()
					#time.sleep(1)	

					s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # creates a TCP object
					s.connect(self.successor) #connect to the other successor
					to_send = "Update_Pred" + "," + str(self.port)
					s.send( to_send.encode("utf-8"))

					msg = s.recv(1024)
					reply =  msg.decode("utf-8") 
					s.close()

					self.Update_Second()
					self.Call()
					#print(self.second)
					time.sleep(3)			

			if self.stop == True:
				return

				



			
		
