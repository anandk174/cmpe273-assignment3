from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys


class MulticastPingClient(DatagramProtocol):
    command_list=[
        'foo:$10',
        'bar:$30',
        'foo:$20',
        'bar:$20',
        'foo:$30',
        'bar:$10'
    ]
    def startProtocol(self):
        # Join the multicast address, so we can receive replies:
        self.transport.joinGroup("228.0.0.5")
        # Send to 228.0.0.5:9999 - all listeners on the multicast address
        # (including us) will receive this message.
        for command in self.command_list:
            command_bytes = bytes('1000 '+command, 'utf-8')
            self.transport.write(command_bytes, ("228.0.0.5", int(sys.argv[1])))

    def datagramReceived(self, datagram, address):
        print("Datagram %s received from %s" % (repr(datagram), repr(address)))


reactor.listenMulticast(9999, MulticastPingClient(), listenMultiple=True)
reactor.run()