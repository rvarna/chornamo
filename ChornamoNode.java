import java.util.Map;

public class ChornamoNode implements Chornamo.Iface {

    private String predecessor;
    private String successor;

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
        return 0;
    }

    public ChornamoStatus replicate_put(String key, String value) {
        return ChornamoStatus.OK;
    }

    public ChornamoStatus kv_put_keys(Map<String, String> input_dict) {
        return ChornamoStatus.OK;
    }

}