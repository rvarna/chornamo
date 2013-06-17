enum ChornamoStatus {
  OK = 1,
  ERROR = 2
}

struct DataResponse {
  1: ChornamoStatus status,
  2: map<string, string> kvstore,
  3: list<string> successor_list
}

struct GetResponse {
  1: ChornamoStatus status,
  2: string value
}

struct SuccessorListResponse {
  1: ChornamoStatus status,
  2: list<string> successor_list
}

service Chornamo {
  GetResponse get(1: string key),
  GetResponse kv_get(1: string key),
  string get_predecessor(),
  string get_successor(),
  string get_successor_for_hashcode(1: string hashcode),
  SuccessorListResponse get_successor_list(),
  DataResponse get_init_data(1: string hash),
  ChornamoStatus put(1: string key, 2: string value),
  ChornamoStatus kv_put(1:string key, 2: string value),
  ChornamoStatus notify(1: string node),
  ChornamoStatus notify_predecessor(1: string node),
  void print_details(),
  void print_successor_list(),
  ChornamoStatus ping(),
  i64 get_key_count(),
  ChornamoStatus replicate_put(1: string key, 2: string value),
  ChornamoStatus kv_put_keys(1: map<string, string> input_dict)
}

