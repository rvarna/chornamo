import java.util.Map;
import java.util.HashMap;
import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;
import java.security.NoSuchAlgorithmException;

public class ChornamoNode implements Chornamo.Iface {

    private String predecessor;
    private String successor;
    private Map<String, String> kv_store;
    private List<String> successor_list;
    private Map<String, String> finger_table;
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
        kv_store = new HashMap<String, String>();
        successor_list = new ArrayList<String>();
        finger_table = new HashMap<String, String>();
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

    public GetResponse kv_get(String key) {
        GetResponse response = new GetResponse();
        response.status = ChornamoStatus.OK;
        return response;
    }

    public String get_predecessor() {
        return predecessor;
    }

    public String get_successor() {
        return successor;
    }


    public String get_successor_for_hashcode(String hashcode) {
        return successor;
    }

    public SuccessorListResponse get_successor_list() {
        return new SuccessorListResponse();
    }

    public DataResponse get_init_data(String hashcode) {
        return new DataResponse();
    }

    public ChornamoStatus put(String key, String value) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus kv_put(String key, String value) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus notify(String successor) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus notify_predecessor(String predecessor) {
        return ChornamoStatus.OK;
    }

    public void print_details() {

    }

    public void print_successor_list() {

    }

    public ChornamoStatus ping() {
        return ChornamoStatus.OK;
    }

    public long get_key_count() {
        return kv_store.size();
    }

    public ChornamoStatus replicate_put(String key, String value) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus kv_put_keys(Map<String, String> input_dict) {
        return ChornamoStatus.OK;
    }

}