enum ChornamoStatus {
  OK = 1,
  ERROR = 2
}

struct DataResponse {
  1: ChornamoStatus status,
  2: map<string, string> kvStore,
  3: list<string> successorList
}

struct GetResponse {
  1: ChornamoStatus status,
  2: string value
}

struct SuccessorListResponse {
  1: ChornamoStatus status,
  2: list<string> successorList
}

service Chornamo {
  GetResponse get(1: string key),
  GetResponse kvGet(1: string key),
  string getPredecessor(),
  string getSuccessor(),
  string getSuccessorForHashcode(1: string hashcode),
  SuccessorListResponse getSuccessorList(),
  DataResponse getInitData(1: string hash),
  ChornamoStatus put(1: string key, 2: string value),
  ChornamoStatus kvPut(1:string key, 2: string value),
  ChornamoStatus notify(1: string node),
  ChornamoStatus notifyPredecessor(1: string node),
  void printDetails(),
  void printSuccessorList(),
  ChornamoStatus ping(),
  i64 getKeyCount(),
  ChornamoStatus replicatePut(1: string key, 2: string value),
  ChornamoStatus kvPutKeys(1: map<string, string> inputMap)
}

