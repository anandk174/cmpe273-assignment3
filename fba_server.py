from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys
import pickledb



db = pickledb.load('assignment3_'+sys.argv[1]+'.db', False)
class MulticastPingPong(DatagramProtocol):
    servers = [3000,3001,3002,3003]
    vote=0
    message_count={}
    commited_messages=[]
    primary_node=False
    quorum=2
    
    def startProtocol(self):
        """
        Called after protocol has started listening.
        """
        # Set the TTL>1 so multicast will cross router hops:
        self.transport.setTTL(5)
        # Join a specific multicast group:
        self.transport.joinGroup("228.0.0.5")

        

    def command(self, datagram, address):        
        received_message = datagram.decode("utf-8")
        print('\nmessage type: Command')
        message = received_message[5:]
        print('message:',message)

        try:
            if (message in self.message_count):
                self.message_count[message]+=1
            else:
                self.message_count[message]=1
        except Exception as e:
            print(e)

        self.primary_node=True
        received_message = received_message.replace('1000', '1001')

        for server in self.servers:
            if not (server == int(sys.argv[1])):
                bytes_to_write = bytes(received_message, 'utf-8')
                self.transport.write(bytes_to_write,("228.0.0.5",server))

    def pre_prepare(self, datagram, address):
        received_message = datagram.decode("utf-8")
        print('\nmessage type: pre_prepare')
        print('message address:', address)
        message = received_message[5:]
        print('message:',message)

        if (message in self.message_count):
            self.message_count[message]+=1
        else:
            self.message_count[message]=1
        
        
        if(not(self.primary_node)):
            received_message = received_message.replace('1001', '1002')
        
            for server in self.servers:
                if not (server == int(sys.argv[1])):
                    bytes_to_write = bytes(received_message, 'utf-8')
                    self.transport.write(bytes_to_write,("228.0.0.5",server))
        
    
    def prepare(self, datagram, address):
        received_message = datagram.decode("utf-8")
        print('\nmessage type: prepare')
        print('message address:', address)
        message = received_message[5:]
        print('message:',message)

        if (message in self.message_count):
            self.message_count[message]+=1
        else:
            self.message_count[message]=1
        
        
        if(not(self.primary_node)):
            received_message = received_message.replace('1002', '1003')
        
            for server in self.servers:
                if not (server == int(sys.argv[1])):
                    bytes_to_write = bytes(received_message, 'utf-8')
                    self.transport.write(bytes_to_write,("228.0.0.5",server))    
    
    def commit(self, datagram, address):
        received_message = datagram.decode("utf-8")
        datagram_message = received_message[5:]
        
        if (self.message_count[datagram_message] >= self.quorum) and (datagram_message not in self.commited_messages):
            mssg_parts = datagram_message.split(':$')
            if db.get(mssg_parts[0]):
                val = int(db.get(mssg_parts[0]))
                val+= int(mssg_parts[1])
                db.set(mssg_parts[0],str(val))                
            else:
                db.set(mssg_parts[0],mssg_parts[1])
            db.dump()
            
            self.commited_messages.append(datagram_message)
            
            print('\ndb update:', mssg_parts[0],'-> $',db.get(mssg_parts[0]))
            print('\ndb status')
            db_dict = db.getall()
            for k in db_dict:
                print(k,' -> ',db.get(k))

            command_implemented = 'Port: '+sys.argv[1]+' implemented '+ datagram_message
            command_implemented = bytes(command_implemented,'utf-8')
            self.transport.write(command_implemented,("228.0.0.5",9999))
            



    def datagramReceived(self, datagram, address):
        
        received_message = datagram.decode("utf-8")
        message_code = received_message[0:4]
        

        if(message_code=='1000'):
            self.command(datagram,address)
        elif(message_code=='1001'):
            self.pre_prepare(datagram,address)
        elif(message_code=='1002'):
            self.prepare(datagram,address)
        elif(message_code=='1003'):
            self.commit(datagram,address)
         
        
            
        


# We use listenMultiple=True so that we can run MulticastServer.py and
# MulticastClient.py on same machine:
reactor.listenMulticast(int(sys.argv[1]), MulticastPingPong(), listenMultiple=True)
reactor.run()