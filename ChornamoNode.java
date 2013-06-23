import java.util.Map;
import java.util.HashMap;
import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

// Thrift imports
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ChornamoNode implements Chornamo.Iface {

    private String predecessor;
    private String successor;
    private Map<String, String> kvStore;
    private List<String> successorList;
    private List<String> fingerTable;
    private String hostname;
    private int port;
    private BigInteger selfHashcode;
    private String nodeKey;
    private Thread stabilizer;
    private final static int SUCCESSOR_LIST_LENGTH = 5;
    private final static int FINGER_TABLE_LENGTH = 256;
    private final static BigInteger MAX = Utils.getPower(2, FINGER_TABLE_LENGTH);
    private enum Operation {
        GET,
        KV_GET,
        PUT, 
        KV_PUT,
        KV_PUT_KEYS,
        REPLICATE_PUT, 
        GET_PREDECESSOR,
        GET_SUCCESSOR,
        GET_SUCCESSOR_FOR_HASHCODE,
        GET_SUCCESSOR_LIST,
        GET_INIT_DATA,
        NOTIFY,
        NOTIFY_PREDECESSOR,
        PING
    };

    public ChornamoNode(String selfName, String selfPort) {
        this(selfName, selfPort, null, null);
    }

    public ChornamoNode(String selfName, String selfPort, String chordNode, String chordPort) {
        predecessor = null;
        hostname = selfName;
        port = Integer.parseInt(selfPort);
        nodeKey = Utils.encode_node(hostname, port);
        successor = nodeKey;
        selfHashcode = Utils.getHash(nodeKey);
        kvStore = new HashMap<String, String>();
        successorList = new ArrayList<String>();
        fingerTable = new ArrayList<String>();
        if (chordNode != null) {
            try {
                Object response = doRemoteOperation(chordNode, Operation.GET_SUCCESSOR_FOR_HASHCODE, Utils.getHash(nodeKey).toString());
                if (response == null) {
                    System.out.println("Failed to obtain successor during initialization. Exiting..");
                    System.exit(1);
                }
                successor = ((String) response);
            } catch (Exception e) {
                System.out.println("Exception during initialization. Exiting..");
                System.exit(1);
            }

            initializeFromSuccessor();
        }
        else {
            initializeSuccessorList();
        }

        initializeFingerTable();
        initializeThreads();
    }

    private void initializeSuccessorList() {
        for (int i = 0; i < SUCCESSOR_LIST_LENGTH; i++) {
            successorList.add(nodeKey);
        }
    }

    private void initializeFingerTable() {
        for (int i = 0; i < FINGER_TABLE_LENGTH; i++) {
            if (successor.equals(nodeKey)) {
                successorList.add(nodeKey);
            }
            else {
                String hashkey = selfHashcode.add(Utils.getPower(2, i)).mod(MAX).toString();
                Object response = doRemoteOperation(successor, Operation.GET_SUCCESSOR_FOR_HASHCODE, hashkey);
                if (response == null) {
                    System.out.println("Unable to initialize finger table. Exiting..");
                    System.exit(1);
                }

                fingerTable.add((String) response);
            }
        }
    }           

    private void initializeFromSuccessor() {
        Object response = doRemoteOperation(successor, Operation.GET_INIT_DATA, nodeKey);
        if (response == null) {
            System.out.println("Unable to obtian init data. Exiting..");
            System.exit(1);
        }

        DataResponse data = (DataResponse) response;
        kvStore = data.kvStore;
        successorList = data.successorList;
        // Remove the last entry in the list and add the successor at the beginning 
        // of the list and list update is complete
        successorList.remove(successorList.size() - 1);
        successorList.add(0, successor);

        // TODO: make notifyPredecessor work.

        response = doRemoteOperation(successor, Operation.NOTIFY, nodeKey);
        if (response == null) {
            System.out.println("Unable to notify successor. Exiting..");
            System.exit(1);
        }

        GetResponse res = (GetResponse) response;
        if (res.status == ChornamoStatus.ERROR) {
            System.out.println("Failure in notifying successor. Exiting..");
            System.exit(1);
        }
    }

    private void initializeThreads() {
        stabilizer = new Thread(new Stabilizer(), "Stabilizer");
        stabilizer.start();
    }

    private GetResponse getFromSuccessorWithRetry(String key) {
        // TODO: Change this if we are not exiting.
        while (true) {
            try {
                Object response = doRemoteOperation(successor, Operation.KV_GET, key);
                if (response == null) {
                    handleSuccessorFailure();
                }
                else {
                    return (GetResponse) response;
                }
            } catch (Exception e) {

            }
        }
    }

    private GetResponse getWithRetry(String masterNode, String key) {
        while (!(masterNode.equals(nodeKey))) {
            try {
                Object response = doRemoteOperation(masterNode, Operation.GET, key);
                if (response == null) {
                    // TODO: improve this.
                    handleSuccessorFailure();
                    masterNode = successor;
                }
                else {
                    return (GetResponse)response;
                }
            } catch (Exception e) {

            }
        }

        GetResponse response = new GetResponse();
        response.status = ChornamoStatus.ERROR;
        return response;
    }

    public GetResponse get(String key) {
        String masterNode = getSuccessorForHashcode(Utils.getHash(key).toString());

        if (masterNode.equals(nodeKey)) {
            return kvGet(key);
        }
        else if (masterNode.equals(successor)) {
            return getFromSuccessorWithRetry(key);
        }
        
        return getWithRetry(masterNode, key);
    }

    public GetResponse kvGet(String key) {
        GetResponse response = new GetResponse();
        if (!kvStore.containsKey(key)) {
            response.status = ChornamoStatus.KEY_NOT_FOUND;
            return response;
        }

        response.status = ChornamoStatus.OK;
        response.value = kvStore.get(key);
        return response;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public String getSuccessor() {
        return successor;
    }

    private int getBestGuess(BigInteger hashcodeInt) {
        // TODO: use lock?
        for (int i = FINGER_TABLE_LENGTH - 1; i > -1; i--) {
            String node = fingerTable.get(i);
            BigInteger nodeHash = Utils.getHash(node);
            if (Utils.isHashcodeBetween(Utils.getHash(node), selfHashcode, hashcodeInt)) {
                return i;
            }
        }

        return -1;
    }

    private String getRemoteSuccessorForKeyWithRetry(int index, String hashcode) {

        String targetNode = (index > -1) ? fingerTable.get(index) : nodeKey;

        while (!(targetNode.equals(nodeKey))) {
            try {
                Object response = doRemoteOperation(targetNode, Operation.GET_SUCCESSOR_FOR_HASHCODE, hashcode);
                if (response == null) {
                    index = handleFingerTableFailure(index);
                    targetNode = fingerTable.get(index);
                }
                else {
                    return (String)response;
                }
            } catch (Exception e) {

            }
        }

        return nodeKey;
    }

    public String getSuccessorForHashcode(String hashcode) {
        if (successor.equals(nodeKey)) {
            return successor;
        }

        BigInteger hashcodeInt = new BigInteger(hashcode);
        // TODO: Use lock?
        if (Utils.isHashcodeBetween(hashcodeInt, selfHashcode, Utils.getHash(successor))) {
            return successor;
        }

        int index = getBestGuess(hashcodeInt);
        return getRemoteSuccessorForKeyWithRetry(index, hashcode);

    }

    public SuccessorListResponse getSuccessorList() {
        SuccessorListResponse response = new SuccessorListResponse();
        response.successorList = successorList;
        response.status = ChornamoStatus.OK;
        return response;
    }

    private Map<String, String> getKVStoreForHashcode(BigInteger hashcode) {
        Map<String, String> returnMap = new HashMap<String, String>();
        // TODO: Initializing predecessor to self might save this branch and the branch in stabilizer.
        if (predecessor == null) {
            for (Map.Entry<String, String> entry : kvStore.entrySet()) {
                BigInteger keyHash = Utils.getHash(entry.getKey());
                if (Utils.isHashcodeBetween(keyHash, selfHashcode, hashcode)) {
                    returnMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        else {
            for (Map.Entry<String, String> entry : kvStore.entrySet()) {
                BigInteger keyHash = Utils.getHash(entry.getKey());
                BigInteger predHash = Utils.getHash(predecessor);
                if (Utils.isHashcodeBetween(keyHash, predHash, hashcode)) {
                    returnMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return returnMap;
    }

    public DataResponse getInitData(String node) {
        DataResponse response = new DataResponse();
        response.kvStore = getKVStoreForHashcode(Utils.getHash(node));
        response.successorList = successorList;
        response.status = ChornamoStatus.OK;
        return response;
    }

    private ChornamoStatus putOnSuccessorWithRetry(String key, String value) {
        // TODO: Change this if we are not exiting.
        while (true) {
            try {
                Object response = doRemoteOperation(successor, Operation.KV_PUT, key, value);
                if (response != null) {
                    return ChornamoStatus.OK;
                }
                else {
                    handleSuccessorFailure();
                }
            } catch (Exception e) {

            }
        }
    }

    private ChornamoStatus putWithRetry(String masterNode, String key, String value) {
        while (!(masterNode.equals(nodeKey))) {
            try {
                // This is an important difference between this method and 
                // put_on_successor_with_retry which tries to directly add in
                // the successor.
                Object response = doRemoteOperation(masterNode, Operation.PUT, key, value);
                if (response == null) {
                    // TODO: improve this.
                    handleSuccessorFailure();
                    // masterNode = getSuccessorForHashcode(Utils.getHash(key));
                    masterNode = successor;
                }
                else {
                    return ChornamoStatus.OK;
                }
            } catch (Exception e) {

            }
        }

        // TODO : use lock?
        kvStore.put(key, value);
        return ChornamoStatus.OK;
    }

    public ChornamoStatus put(String key, String value) {
        String masterNode = getSuccessorForHashcode(Utils.getHash(key).toString());

        if (masterNode.equals(nodeKey)) {
            return kvPut(key, value);
        }
        else if (masterNode.equals(successor)) {
            return putOnSuccessorWithRetry(key, value);
        }
        return putWithRetry(masterNode, key, value);
    }

    public ChornamoStatus kvPut(String key, String value) {
        // TODO: Use lock?
        kvStore.put(key, value);
        replicateKey(key, value);
        return ChornamoStatus.OK;
    }

    public ChornamoStatus notify(String node) {
        predecessor = node;
        return ChornamoStatus.OK;
    }

    public ChornamoStatus notifyPredecessor(String node) {
        successor = node;
        return ChornamoStatus.OK;
    }

    public void printDetails() {

    }

    public void printSuccessorList() {

    }

    public ChornamoStatus ping() {
        return ChornamoStatus.OK;
    }

    public long getKeyCount() {
        return kvStore.size();
    }

    private void replicateKey(String key, String value) {
        for (String node : successorList) {
            if (node.equals(nodeKey)) {
                continue;
            }
            try {
                doRemoteOperation(node, Operation.REPLICATE_PUT, key, value);
            } catch (Exception e) {
                // May be log it. Otherwise, it is ok if it fails. Stabilize will take care of it.
            }
        }
    }

    public ChornamoStatus replicatePut(String key, String value) {
        // TODO: Use lock?
        kvStore.put(key, value);
        return ChornamoStatus.OK;
    }

    public ChornamoStatus kvPutKeys(Map<String, String> inputDict) {
        for (Map.Entry<String, String> entry : inputDict.entrySet()) {
            kvStore.put(entry.getKey(), entry.getValue());
        }

        return ChornamoStatus.OK;
    }

    private void replicateKVStore(String targerNode) {
        try{
            doRemoteOperation(targerNode, Operation.KV_PUT_KEYS, kvStore);
        } catch (Exception e) {
            // The node probably failed. Will be handled in stabilizer.
        }
    }

    private void replicateToNewSuccessors(List<String> oldSuccessorList) {
        for (String node : successorList) {
            if (oldSuccessorList.contains(node)) {
                continue;
            }

            replicateKVStore(node);
        }
    }

    private Object doRemoteOperation(String hostWithPort, Operation op) {
        return doRemoteOperation(hostWithPort, op, null, null, null);
    }

    private Object doRemoteOperation(String hostWithPort, Operation op, String key) {
        return doRemoteOperation(hostWithPort, op, key, null, null);
    }

    private Object doRemoteOperation(String hostWithPort, Operation op, String key, String value) {
        return doRemoteOperation(hostWithPort, op, key, value, null);
    }

    private Object doRemoteOperation(String hostWithPort, Operation op, Map<String, String> map) {
        return doRemoteOperation(hostWithPort, op, null, null, map);
    }

    private Object doRemoteOperation(String hostWithPort, Operation op, String key, String value, Map<String, String> map) {
        TTransport transport;
        String[] splitString = Utils.decodeNode(hostWithPort);
        try {
            transport = new TFramedTransport(new TSocket(splitString[0], Integer.parseInt(splitString[1])));
            TProtocol protocol = new TBinaryProtocol(transport);
 
            Chornamo.Client client = new Chornamo.Client(protocol);
            transport.open();
            Object response = null;

            switch(op) {
                case GET:
                    response = client.get(key);
                    break;
                case KV_GET:
                    response = client.kvGet(key);
                    break;

                case PUT:
                    response = client.put(key, value);
                    break;

                case KV_PUT:
                    response = client.kvPut(key, value);
                    break;

                case GET_SUCCESSOR:
                    response = client.getSuccessor();
                    break;

                case GET_PREDECESSOR:
                    response = client.getPredecessor();
                    break;

                case GET_INIT_DATA:
                    response = client.getInitData(key);
                    break;

                case NOTIFY:
                    response = client.notify(key);
                    break;

                case NOTIFY_PREDECESSOR:
                    response = client.notifyPredecessor(key);
                    break;

                case GET_SUCCESSOR_LIST:
                    response = client.getSuccessorList();
                    break;

                case GET_SUCCESSOR_FOR_HASHCODE:
                    response = client.getSuccessorForHashcode(key);
                    break;

                default:
                    break;
            }

            transport.close();
            return response;

        } catch (TTransportException e) {
            e.printStackTrace();
            // TODO: Add specific error codes or null response is sufficient?
        } catch (TException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
    private synchronized void handleSuccessorFailure() {
        // TODO : Use lock?
        Iterator<String> iter = successorList.iterator();
        while(iter.hasNext()) {
            String node = iter.next();
            if (node.equals(nodeKey)) {
                continue;
            }

            Object response = doRemoteOperation(node, Operation.GET_SUCCESSOR_LIST);
            if (response == null) {
                continue;
            }

            successor = node;
            response = doRemoteOperation(node, Operation.NOTIFY, nodeKey);
            if (response == null) {
                continue;
            }

            List<String> oldSuccessorList = successorList;
            successorList = ((SuccessorListResponse)response).successorList;
            successorList.remove(successorList.size() - 1);
            successorList.add(0, successor);

            replicateToNewSuccessors(oldSuccessorList);
            return;
        }

        System.out.println(" Network Partition or large number of failures. Exiting ..");
        System.exit(1);
    }


    private synchronized int handleFingerTableFailure(int index) {
        String deadNode = fingerTable.get(index);
        int i = index - 1;
        while (i > -1) {
            if (fingerTable.get(i).equals(nodeKey)) {
                i -= 1;
            }
            else 
                break;
        }

        int aliveIndex = -1;
        for (int j = i; j > -1; j--) {
            Object response = doRemoteOperation(fingerTable.get(j), Operation.PING);
            if (response == null) {
                continue;
            }
            else {
                aliveIndex = j;
                break;
            }
        }

        String aliveNode = fingerTable.get(aliveIndex);
        for (int j = aliveIndex; j <= index; j++) {
            fingerTable.set(j, aliveNode);
        }

        return aliveIndex;
    }


    private class Stabilizer implements Runnable {
        public void run() {
            while (true) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {

                }

                stabilize();
                fixFingerTables();
            }
        }

        private void stabilize() {
            if (successor.equals(nodeKey)) {
                if (predecessor != null && !(successor.equals(predecessor))) {
                    successor = predecessor;
                    doRemoteOperation(predecessor, Operation.NOTIFY, nodeKey);
                }
            }
            else {
                Object response = doRemoteOperation(successor, Operation.GET_PREDECESSOR);
                if (response == null) {
                    handleSuccessorFailure();
                    return;
                }

                String newPredecessor = (String)response;
                if (!(nodeKey.equals(newPredecessor))) {
                    // A new node has joined and is the current node's successor.
                    response = doRemoteOperation(newPredecessor, Operation.NOTIFY);
                    if (response == null) {
                        return;
                    }

                    successor = newPredecessor;
                }

            }

            if (!(successor.equals(nodeKey))) {
                Object response = doRemoteOperation(successor, Operation.GET_SUCCESSOR_LIST);
                if (response == null) {
                    handleSuccessorFailure();
                    return;
                }

                List<String> oldSuccessorList = successorList;
                successorList = ((SuccessorListResponse)response).successorList;
                successorList.remove(successorList.size() - 1);
                successorList.add(0, successor);

                replicateToNewSuccessors(oldSuccessorList);
            }

            
            //printDetails()
            //printSuccessorList()
        }

        private void fixFingerTables() {
            for (int i = 0; i < FINGER_TABLE_LENGTH; i++) {
                String hashkey = selfHashcode.add(Utils.getPower(2, i)).mod(MAX).toString();
                fingerTable.set(i, getSuccessorForHashcode(hashkey));
            }
        }
    }
}