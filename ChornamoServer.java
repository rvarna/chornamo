import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;

public class ChornamoServer {
 
    private void start() {
        try {
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(7911);
            Chornamo.Processor processor = new Chornamo.Processor(new ChornamoNode());
 
            TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).
                    processor(processor));
            System.out.println("Starting server on port 7911 ...");
            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }
 
    public static void main(String[] args) {
        ChornamoServer srv = new ChornamoServer();
        srv.start();
    }
}