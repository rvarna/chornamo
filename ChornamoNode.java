import java.util.Map;
import java.util.HashMap;
import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;
import java.security.NoSuchAlgorithmException;

public class ChornamoNode implements Chornamo.Iface {

    private String predecessor;
    private String successor;
    private Map<String, String> kvStore;
    private List<String> successorList;
    private Map<String, String> fingerTable;
    private String hostname;
    private int port;
    private BigInteger selfHashcode;
    private String nodeKey;

    private final static int SUCCESSOR_LIST_LENGTH = 5;
    private final static int FINGER_TABLE_LENGTH = 256;

    public ChornamoNode(String selfNode, String selfPort, String chordNode, String chordPort) {
        predecessor = null;
        hostname = selfNode;
        port = Integer.parseInt(selfPort);
        nodeKey = Utils.encode_node(hostname, port);
        successor = nodeKey;
        selfHashcode = Utils.getHash(nodeKey);
        kvStore = new HashMap<String, String>();
        successorList = new ArrayList<String>();
        fingerTable = new HashMap<String, String>();
    }

    public ChornamoNode(String selfNode, String selfPort) {
        this(selfNode, selfPort, null, null);
    }
    private void initializeThreads() {

    }

    public GetResponse get(String key) {
        GetResponse response = new GetResponse();
        response.status = ChornamoStatus.OK;
        return response;
    }

    public GetResponse kvGet(String key) {
        GetResponse response = new GetResponse();
        response.status = ChornamoStatus.OK;
        return response;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public String getSuccessor() {
        return successor;
    }


    public String getSuccessorForHashcode(String hashcode) {
        return successor;
    }

    public SuccessorListResponse getSuccessorList() {
        return new SuccessorListResponse();
    }

    public DataResponse getInitData(String hashcode) {
        return new DataResponse();
    }

    public ChornamoStatus put(String key, String value) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus kvPut(String key, String value) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus notify(String successor) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus notifyPredecessor(String predecessor) {
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

    public ChornamoStatus replicatePut(String key, String value) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus kvPutKeys(Map<String, String> input_dict) {
        return ChornamoStatus.OK;
    }

    private class Stabilizer implements Runnable {
        public void run() {
            stabilize();
            fixFingerTables();
        }

        private void stabilize() {

        }

        private void fixFingerTables() {

        }
    }

    private synchronized void handleSuccessorFailure() {

    }


    private synchronized void handleFingerTableFailure() {

    }

}