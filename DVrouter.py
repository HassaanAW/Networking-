import sys
from collections import defaultdict
from router import Router
from packet import Packet
from json import dumps, loads


class DVrouter(Router):
    """Distance vector routing protocol implementation."""

    def __init__(self, addr, heartbeatTime):
        """TODO: add your own class fields and initialization code here"""
        Router.__init__(self, addr)  # initialize superclass - don't remove
        self.heartbeatTime = heartbeatTime
        self.last_time = 0

        self.address = addr
        # initilize a neighbor structure, a forwarding table and a DV

        self.neighbor_port = {}
        self.port_neighbor = {}
        self.neighbor_cost = {}

        self.distance_vector = {}
        self.forwarding_table = {}

        self.distance_vector.update({self.address: (0, None) })
        self.forwarding_table.update({ self.address: 0 })
        # Hints: initialize local state
        pass

    def handlePacket(self, port, packet):
        """TODO: process incoming packet"""
        if packet.isTraceroute():
            #print("Iam", self.address, "DV", self.distance_vector)
            if packet.dstAddr in self.distance_vector:
                if self.distance_vector[packet.dstAddr][0] < 16:
                    next_hop = self.distance_vector[packet.dstAddr][1]
                    if self.neighbor_cost[next_hop] < 16:
                        self.send(self.neighbor_port[next_hop], packet)

        else:

            check = 0
            retrieve = loads(packet.content)
            gen = packet.srcAddr

            # in case the next hop of a destination was same as the generator of the packet
            # check if generator->destination has changed  
            for key, value in retrieve.items():
                if key in self.distance_vector:
                    if self.distance_vector[key][1] == gen:
                        new_cost = value + self.neighbor_cost[gen]
                        old_cost = self.distance_vector[key][0]
                        if old_cost != new_cost:
                            if new_cost >= 16:
                                self.distance_vector.update({key: (16,None) })
                                self.forwarding_table.update({key: 16})
                            else:
                                self.distance_vector.update({key: ( new_cost,gen) })
                                self.forwarding_table.update({key: new_cost})
                            check = 1
                        else:
                            pass
            # normal updation
            for key, value in retrieve.items():
                if key not in self.distance_vector:
                    cost = value + self.neighbor_cost[gen] # self.distance_vector[gen][0] -> cost to reach neighbor
                    self.distance_vector.update({key: (cost, gen) })
                    self.forwarding_table.update({key: cost })
                    check = 1
                else:
                    new_cost = value + self.neighbor_cost[gen]
                    old_cost = self.distance_vector[key][0]
                    if old_cost > new_cost:
                        self.distance_vector.update({key: (new_cost, gen )})
                        self.forwarding_table.update({key:new_cost})
                        check = 1
                    else:
                        pass             

            # Now broadcast
            if(check == 1): # there's new info
                for key in self.neighbor_port:
                    new_packet = Packet(Packet.ROUTING, self.address, key)
                    new_packet.content = dumps(self.forwarding_table)
                    if self.neighbor_cost[key] < 16:
                        self.send(self.neighbor_port[key], new_packet)
            


    def handleNewLink(self, port, endpoint, cost):
        """TODO: handle new link"""

        self.neighbor_port.update({endpoint:port})
        self.port_neighbor.update({port:endpoint})
        self.neighbor_cost.update({endpoint:cost})

        check = 0
        if endpoint not in self.distance_vector:
            self.distance_vector.update({endpoint:(cost,endpoint) }) # format of DV: { address: (cost, nexthop) }
            self.forwarding_table.update({endpoint: cost})
            check = 1
        
        #else check whether new cost is less than already stored cost. If it is, then update. else dont update in DV
        else: # there is already a path through some other node and now a direct link is created
            if cost < self.distance_vector[endpoint][0]: # better path found; update DV
                self.distance_vector.update({endpoint:(cost,endpoint) })
                self.forwarding_table.update({endpoint: cost})
                check = 1

        # broadcast FT now
        if check == 1:
            for key in self.neighbor_port:
                packet = Packet(Packet.ROUTING, self.address, key)
                packet.content = dumps(self.forwarding_table)
                if self.neighbor_cost[key] < 16:
                    self.send(self.neighbor_port[key], packet)

        #print("I am", self.address, "FT", self.forwarding_table, "DV", self.distance_vector)


    def handleRemoveLink(self, port):
        """TODO: handle removed link"""
        # update the distance vector of this router
        # update the forwarding table
        # broadcast the distance vector of this router to neighbors
               
        #pSrint("iam", self.address, "removed with", find_add)

        find_add = self.port_neighbor[port]
        self.neighbor_cost.update({find_add:16})
        self.neighbor_port.update({find_add:None})
        self.distance_vector.update({find_add: (16,None) })
        self.forwarding_table.update({find_add: 16 })

        # in case a next hop for a value is the link removed
        for key, value in self.distance_vector.items():
            next_hop = self.distance_vector[key][1]
            if next_hop == find_add:
                self.distance_vector.update({key :(16,None) })
                self.forwarding_table.update({key : 16}) 

        for key in self.neighbor_port:
            packet = Packet(Packet.ROUTING, self.address, key)
            packet.content = dumps(self.forwarding_table)
            if self.neighbor_cost[key] < 16:
                self.send(self.neighbor_port[key], packet)


    def handleTime(self, timeMillisecs):
        """TODO: handle current time"""
        if timeMillisecs - self.last_time >= self.heartbeatTime:
            self.last_time = timeMillisecs
            # broadcast the distance vector of this router to neighbors
            for key in self.neighbor_port:
                packet = Packet(Packet.ROUTING, self.address, key)
                packet.content = dumps(self.forwarding_table)

                if self.neighbor_cost[key] < 16:
                    self.send(self.neighbor_port[key], packet)                

    def debugString(self):
        """TODO: generate a string for debugging in network visualizer"""
        return ""
