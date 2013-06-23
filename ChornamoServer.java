import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;

/*
    Adapted from:
    http://chamibuddhika.wordpress.com/2011/10/02/apache-thrift-quickstart-tutorial/
*/
public class ChornamoServer {
    public static void main(String[] args) {
        try {
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(Integer.parseInt(args[1]));
            Chornamo.Processor processor;
            if (args.length > 3) {
                processor = new Chornamo.Processor(new ChornamoNode(args[0], args[1], args[2], args[3]));
            }
            else {
                processor = new Chornamo.Processor(new ChornamoNode(args[0], args[1]));
            }
 
            TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).
                    processor(processor));
            System.out.println("Starting server on port " + args[1]);
            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }
}