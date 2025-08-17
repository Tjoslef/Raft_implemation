#include <condition_variable>
#include <cwchar>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#include <chrono>
#include <iostream>
#include <random>
#include <fstream>
#include <unistd.h>
#include <zlib.h>
#include <fcntl.h>
#include <sys/stat.h>
enum Status{
    Leader = 0,
    Candidate = 1,
    Follower = 2
};
enum Op : uint8_t {
    NoOp=0,
    Put=1,
    Del=2
};
enum class MessageType {
    VoteRequest,
    VoteResponse,
    AppendEntries,
    AppendEntriesResponse
};
struct LogEntry{
    int term;
    Op op;
    std::string key;
    std::string command;
};
struct VoteRequest {
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};
#pragma pack(push, 1)
struct WalRecordHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t payload_len;
    uint32_t crc32;
    uint64_t term;
};
#pragma pack(pop)

static constexpr uint32_t WAL_MAGIC = 0xDEC0AD01;
static constexpr uint32_t WAL_VERSION = 1;
struct VoteResponse {
    int term;
    bool voteGranted = false;

};
//heartbeat and log from leader
struct AppendEntries {
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<LogEntry> entries;
    int leaderCommit;
};
//answer to log
struct AppendEntriesResponse {
    int responderId;
    int term;
    bool success;
    int conflictTerm = -1;
    int conflictIndex = -1;
    int logIndex = -1;
};
struct HardState {
    uint32_t magic;
    uint32_t version;
    uint64_t currentTerm;
    uint64_t votedFor;
    uint32_t checksum;
};
struct Message {
    MessageType type;
    std::variant<VoteRequest, VoteResponse, AppendEntries, AppendEntriesResponse> data;
};
class StateMachine {
public:
void apply(std::vector<LogEntry> &logs);
private:
std::unordered_map<std::string,std::string > kv;
};
class RaftNode {
    public:
    int id;
    std::string data_dir, hard_state_path, wal_path;
    explicit RaftNode(int nodeId);
        RaftNode()
        : rand_gen(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
          electionTimeoutDistribution(100, 600),
          heartbeatIntervalDistribution(100, 600) {
          }
        std::chrono::milliseconds getElectionTimeout() {
            return std::chrono::milliseconds(electionTimeoutDistribution(rand_gen));
            }
        std::chrono::milliseconds getHeartbeatInterval() {
            return std::chrono::milliseconds(heartbeatIntervalDistribution(rand_gen));
            }
        int Loop(const std::vector<std::unique_ptr<RaftNode>>& nodes);
        void startElection(const std::vector<std::unique_ptr<RaftNode>>& nodes);
        VoteResponse receiveVoteRequest(VoteRequest voteReq);
        void onVoteGranted();
        int getLastLogTerm();
        HardState recovery_Hard_state(const std::string& hardPath);
        void receivingMessage(std::vector<std::unique_ptr<RaftNode>> &nodes);
        void sendMessageToNode(int nodeId, const Message& msg, const std::vector<std::unique_ptr<RaftNode>>& nodes);
        std::string messageTypeToString(MessageType type);
        AppendEntries create_entries(int num_log,bool heartbeat);
        void sendAppendEntries(Message msg,const std::vector<std::unique_ptr<RaftNode>>& nodes);
        void processIncomingMessages(const std::vector<std::unique_ptr<RaftNode>>& nodes);
        void checkAndCommitLogs(const std::vector<std::unique_ptr<RaftNode>>& nodes);
        int saveHardState();
        int appendWal(const std::vector<LogEntry> &logs,const std::string& walPath);
        LogEntry deserialize(const std::vector<char>& buffer);
        std::vector<LogEntry>recovery_wal(const std::string& walPath);
        int appendWal(const std::vector<LogEntry> &logs);
        bool init_from_storage();
        void applyCommitted();
        bool checkFile();
    private:
        std::random_device rd;
    std::mt19937 rand_gen;
    std::uniform_int_distribution<> electionTimeoutDistribution;
    std::uniform_int_distribution<> heartbeatIntervalDistribution;
    Status current_status = Status::Follower;
    int currentTerm = 0;
    int votedFor = -1;
    int lastLogIndex = 0;
    std::vector<LogEntry> log;
    int votesGranted = 0;
    int commitIndex = 0;
    int lastApplied = 0;
    std::vector<int> peers;
    std::chrono::milliseconds electionTimeout;
    std::chrono::milliseconds heartbeatInterval;
    std::chrono::steady_clock::time_point nextHeartbeatTime;
    StateMachine stateMachine;
    std::queue<Message> incomingMessages;
    std::mutex mutex_hearbeat;
    std::mutex mutex_hard_state;
    std::mutex mutex_wal;
    std::mutex mutex_election;
    std::mutex mutex_message;
    std::mutex applied_commites;
    std::mutex message_queue_for_loop_mutex;
    std::queue<Message> message_queue_for_loop;
    bool heartbeatReceived = false;
    std::condition_variable cv_heartbeat;
    std::condition_variable cv;
    std::condition_variable cv_incoming_message;
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;
};
