#include "../include/raft_node.hpp"
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <variant>
#include <vector>
#include <zconf.h>
#include <zlib.h>
std::mutex cerr_mutex;
std::mutex cout_mutex;
const std::string& walPath = "./backup/walSave";
RaftNode::RaftNode(int nodeId)
    : id(nodeId),
      rand_gen(rd()),
      electionTimeoutDistribution(400,800 ),
      heartbeatIntervalDistribution(100, 200)
{
    this->electionTimeout = getElectionTimeout();
    this->heartbeatInterval = getHeartbeatInterval();
    std::cout << "RaftNode " << id << " created. ElectionTimeout: " << electionTimeout.count() << " ms, HeartbeatInterval: " << heartbeatInterval.count() << " ms\n";

}

void RaftNode::checkAndCommitLogs(const std::vector<std::unique_ptr<RaftNode>>& nodes) {
    if (this->current_status != Status::Leader) return;
    const size_t n = nodes.size();
    const int lastLogIndex = static_cast<int>(log.size());
    std::vector<int> sortedMatchIndexes;
    if (matchIndex.size() != n) {
        matchIndex.assign(n, 0);
    }
    if (nextIndex.size() != n) {
        nextIndex.assign(n, lastLogIndex + 1);
    }
    sortedMatchIndexes.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        int m = (i < matchIndex.size()) ? matchIndex[i] : 0;
        if (m < 0) m = 0;
        if (m > lastLogIndex) m = lastLogIndex;
        sortedMatchIndexes.push_back(m);
    }

    if (sortedMatchIndexes.empty()) return;
    std::sort(sortedMatchIndexes.begin(), sortedMatchIndexes.end());
    int majorityIndex = sortedMatchIndexes[n / 2];
    if (majorityIndex > lastLogIndex) majorityIndex = lastLogIndex;
    if (majorityIndex > commitIndex && majorityIndex > 0) {
        int vecIdx = majorityIndex - 1; // 0-based
        if (vecIdx >= 0 && vecIdx < static_cast<int>(log.size())) {
            if (log[vecIdx].term == currentTerm) {
                commitIndex = majorityIndex;
                {
                    std::lock_guard<std::mutex> g(applied_commites);
                    applyCommitted();
                }
            }
        }
    }
}
void RaftNode::processIncomingMessages(const std::vector<std::unique_ptr<RaftNode>>& nodes) {
    for(;;) {
        Message msg;
                {
                    std::lock_guard<std::mutex> qlock(mutex_message);
                    if (incomingMessages.empty()) break;
                    msg = std::move(incomingMessages.front());
                    incomingMessages.pop();
                }
                {
                    std::lock_guard<std::mutex> lk(cout_mutex);
                    std::cout << "[Node " << id << "] processing message type " << messageTypeToString(msg.type) << "\n";
                }
        if (msg.type == MessageType::AppendEntries) {
            if(auto temp = std::get_if<AppendEntries>(&msg.data)){
                    electionTimeout = getElectionTimeout();
                    heartbeatInterval = getHeartbeatInterval();
                    {
                        std::lock_guard<std::mutex> hlock(mutex_hearbeat);
                        heartbeatReceived = true;
                    }
                    cv_heartbeat.notify_one();
                    int snapshot_appendWal_status = 0;
                    if (temp->term >= currentTerm) {
                        if (temp->term > currentTerm) {
                            currentTerm = temp->term;
                            votedFor = -1;
                        }
                        current_status = Status::Follower;
                    }else {
                        AppendEntriesResponse response{};
                        response.responderId = this->id;
                        response.term = this->currentTerm;
                        response.success = false;
                        response.logIndex = (int)this->log.size();
                        response.conflictIndex = 0;
                        response.conflictTerm = 0;

                        Message responseMsg;
                        responseMsg.type = MessageType::AppendEntriesResponse;
                        responseMsg.data = response;
                        sendMessageToNode(temp->leaderId, responseMsg, nodes);
                        continue;
                    }
                    AppendEntriesResponse response{};
                    response.responderId = this->id;
                    response.term = this->currentTerm;
                    response.success = true;
                    response.logIndex = (int)this->log.size();
                    response.conflictIndex = 0;
                    response.conflictTerm = 0;
                    if(!temp->entries.empty()){
                        bool log_ok = (this->log.size() >= temp->prevLogIndex &&
                        (temp->prevLogIndex == 0 || this->log[temp->prevLogIndex - 1].term == temp->prevLogTerm));
                    if(!log_ok){
                            response.success = false;
                        if (this->log.size() < temp->prevLogIndex) {
                            response.conflictIndex = this->log.size();
                            response.conflictTerm = 0;
                        } else {
                            response.conflictIndex = temp->prevLogIndex;
                            response.conflictTerm = this->log[temp->prevLogIndex - 1].term;
                            }
                   }else {
                        int index = temp->prevLogIndex; // 1-based
                        for (size_t i = 0; i < temp->entries.size(); ++i) {
                            int li = index + 1 + (int)i; // log index (1-based)
                            if (li <= (int)log.size()) {
                                if (log[li - 1].term != temp->entries[i].term) {
                                    log.erase(log.begin() + (li - 1), log.end());
                                    log.push_back(temp->entries[i]);
                                }
                            } else {
                                log.push_back(temp->entries[i]);
                            }
                        }
                        if (temp->leaderCommit > commitIndex) {
                            commitIndex = std::min(temp->leaderCommit, (int)log.size());
                            {
                            std::lock_guard<std::mutex> Alock(applied_commites);
                           applyCommitted();
                            }
                        }
                    }
                }

                {
                    std::lock_guard<std::mutex> wlock(mutex_wal);
                    if(appendWal(temp->entries) == 0){
                        snapshot_appendWal_status = -1;
                    }
                    }
                {
                    std::lock_guard<std::mutex> Elock(cerr_mutex);
                    if(snapshot_appendWal_status == -1){
                        std::cerr << "error in append backup \n";
                        exit(1);
                    }
                }
                response.logIndex = (int)this->log.size();

                Message responseMsg;
                responseMsg.type = MessageType::AppendEntriesResponse;
                responseMsg.data = response;
                sendMessageToNode(temp->leaderId, responseMsg, nodes);

                {
                    std::lock_guard<std::mutex> lk(cout_mutex);
                    std::cout << "[Node " << id << "] handled AppendEntries from " << temp->leaderId
                                << " success=" << (response.success ? "true" : "false")
                                << " logIndex=" << response.logIndex << "\n";
                }
            }
        } else if (msg.type == MessageType::AppendEntriesResponse) {
            if(auto temp = std::get_if<AppendEntriesResponse>(&msg.data)){
                if (auto temp = std::get_if<AppendEntriesResponse>(&msg.data)) {
                    if (temp->term > this->currentTerm) {
                        this->currentTerm = temp->term;
                        this->current_status = Status::Follower;
                        this->votedFor = -1;
                        continue;
                    }

                    if (this->current_status == Status::Leader) {
                        if (temp->success) {
                            if ((int)this->nextIndex.size() > temp->responderId) {
                                this->nextIndex[temp->responderId] = temp->logIndex + 1;
                            }
                            if ((int)this->matchIndex.size() > temp->responderId) {
                                this->matchIndex[temp->responderId] = temp->logIndex;
                            }
                            checkAndCommitLogs(nodes);
                        } else {
                            if ((int)this->nextIndex.size() > temp->responderId &&
                                this->nextIndex[temp->responderId] > 1) {
                                this->nextIndex[temp->responderId]--;
                            }
                        }

                    }
                }
                {
                    std::lock_guard<std::mutex> lk(cout_mutex);
                    std::cout << "[Node " << id << "] processed AppendEntriesResponse from "
                                << temp->responderId << " success=" << (temp->success ? "true" : "false")
                                << " followerLogIndex=" << temp->logIndex << "\n";
                }
            }

        }else if (msg.type == MessageType::VoteResponse) {
            const VoteResponse voteRes = std::get<VoteResponse>(msg.data);
            int votes_snapshot = 0;
            bool counted = false;
            int term_snapshot = 0;
            Status status_snapshot;

            {
                std::lock_guard<std::mutex> elock(mutex_election);
                status_snapshot = current_status;
                term_snapshot = currentTerm;

                if (status_snapshot == Status::Candidate &&
                    voteRes.voteGranted &&
                    voteRes.term == term_snapshot) {
                    ++votesGranted;
                    counted = true;
                    cv.notify_one();
                }
                votes_snapshot = votesGranted;
            }

            {
                std::lock_guard<std::mutex> lk(cout_mutex);
                std::cout << "[Node " << id << "] VoteResponse: granted="
                          << (votes_snapshot ? "true" : "false")
                          << " counted=" << (counted ? "true" : "false")
                          << " res.term=" << voteRes.term
                          << " cur.term=" << term_snapshot
                          << " status=" << (int)status_snapshot << "\n";
            }
        }else if (msg.type == MessageType::VoteRequest) {
            if (auto temp = std::get_if<VoteRequest>(&msg.data)) {
                VoteResponse response{};
                response.term = currentTerm;
                response.voteGranted = false;

                int voter_log_term = getLastLogTerm();

                if (temp->term > currentTerm) {
                    currentTerm = temp->term;
                    votedFor = -1;
                    current_status = Status::Follower;
                    {
                        std::lock_guard<std::mutex> Flock(mutex_hard_state);
                        saveHardState();
                    }
                }

                bool logUpToDate =
                    (temp->lastLogTerm > voter_log_term) ||
                    (temp->lastLogTerm == voter_log_term && temp->lastLogIndex >= lastLogIndex);

                if ((votedFor == -1 || votedFor == temp->candidateId) &&
                    logUpToDate &&
                    temp->term >= currentTerm) {
                    votedFor = temp->candidateId;
                    response.voteGranted = true;
                    response.term = temp->term;
                    {
                        std::lock_guard<std::mutex> Flock(mutex_hard_state);
                        saveHardState();
                    }
                }

                {
                    std::lock_guard<std::mutex> lk(cout_mutex);
                    std::cout << "[Node " << id << "] VoteRequest from " << temp->candidateId
                                << " term=" << temp->term
                                << " granted=" << (response.voteGranted ? "true" : "false") << "\n";
                }

                Message responseMsg;
                responseMsg.type = MessageType::VoteResponse;
                responseMsg.data = response;
                sendMessageToNode(temp->candidateId, responseMsg, nodes);
            }
        } else {
            {
                std::lock_guard<std::mutex> lk(cout_mutex);
                std::cout << "[Node " << id << "] Unknown message type received\n";
            }
        }
    }
        }
void RaftNode::sendMessageToNode(int nodeId, const Message& msg, const std::vector<std::unique_ptr<RaftNode>>& nodes) {
    if (!(nodeId >= 0 && nodes[nodeId])) {
                std::lock_guard<std::mutex> lock(cerr_mutex);
          std::cerr << "Assertion failed in process_node:" << std::endl;
          std::cerr << "  Condition: nodeId >= 0 && nodes[nodeId]" << std::endl;
          std::cerr << "  Failed with nodeId = " << nodeId << std::endl;
          std::cerr << "  Nodes size: " << nodes.size() << std::endl;
          std::cerr << "    - Message type: " << messageTypeToString(msg.type) << std::endl;
          std::abort();
      }
    auto& node = nodes[nodeId];
    {
        std::lock_guard<std::mutex> lock(node->mutex_message);
        node->incomingMessages.push(msg);
        node->cv_incoming_message.notify_one();
    }
    {
        std::lock_guard<std::mutex> lock(cout_mutex);
   std::cout << "sendMessageToNode " << nodeId << " type of " << messageTypeToString(msg.type) << "\n";
    }
}
int RaftNode::getLastLogTerm()  {
    if (log.empty()) return 0;
    return log.back().term;
}
void RaftNode::startElection(const std::vector<std::unique_ptr<RaftNode>>& nodes) {
   int snapshot_term;
   int snapshot_votesGranted;
   Status snapshot_status;
    {
        std::lock_guard<std::mutex> lock(mutex_election);
        this->current_status = Status::Candidate;
        snapshot_votesGranted = votesGranted++;
        this->currentTerm++;
        snapshot_term = currentTerm;
        this->votedFor = id;
    }
    {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "[Node " << id << "] Starting election for term " << snapshot_term
              << ", votesGranted = " << snapshot_votesGranted << "\n";
    }
    const int lastIdx = (int)log.size();
    const int lastTerm = getLastLogTerm();
    VoteRequest voteReq{ currentTerm, id, lastIdx, lastTerm };
    for (auto& node : nodes) {
        if (node.get() == this) continue;
                Message msg;
                msg.type = MessageType::VoteRequest;
                msg.data = voteReq;
                {
                    std::lock_guard<std::mutex> lk(cout_mutex);
                    std::cout << "[Node " << id << "] Sending " << messageTypeToString(msg.type)
                                << " to Node " << node->id << "\n";
                }
            sendMessageToNode(node->id, msg, nodes);
        }
    bool won = false;
    auto deadline = std::chrono::steady_clock::now() + electionTimeout;
    const size_t majority = nodes.size() / 2 + 1;
    heartbeatInterval = getHeartbeatInterval();
    std::unique_lock<std::mutex> lock(mutex_election);
    while (current_status == Status::Candidate &&
    votesGranted < (int)majority) {
    auto now = std::chrono::steady_clock::now();
    if(now>= deadline){
        break;
    }
    auto slice = std::min(std::chrono::milliseconds(40),
    std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
    cv.wait_for(lock, slice, [&]{
        return votesGranted >= (int)majority || current_status != Status::Candidate;
    });
    if (votesGranted >= (int)majority || current_status != Status::Candidate) break;

    lock.unlock();
    processIncomingMessages(nodes);
    lock.lock();
    }
    if(current_status == Candidate&& votesGranted >= (int)majority ){
        current_status = Status::Leader;
        snapshot_status = current_status;
        const int myLast = (int)log.size();
        if ((int)nextIndex.size() < (int)nodes.size()) nextIndex.resize(nodes.size(), myLast + 1);
        if ((int)matchIndex.size() < (int)nodes.size()) matchIndex.resize(nodes.size(), 0);
        won = true;
        for (auto& n : nodes) {
            if (n.get() == this) continue;
            nextIndex[n->id] = myLast + 1;
            matchIndex[n->id] = 0;
        }
        matchIndex[this->id] = myLast;

        lock.unlock();

        {
            std::lock_guard<std::mutex> lk(cout_mutex);
            std::cout << "[Node " << id << "] Became Leader for term " << snapshot_term
                      << " with votes=" << snapshot_votesGranted << "\n";
        }
    } else {
        lock.unlock();
        {
            std::lock_guard<std::mutex> lk(cout_mutex);
            std::cout << "[Node " << id << "] Election not won (status=" << (int)snapshot_status
                      << ", votes=" << snapshot_votesGranted
                      << ", won=" << (won ? "true" : "false") << ")\n";
        }
    }
}
int RaftNode::Loop(const std::vector<std::unique_ptr<RaftNode>>& nodes) {
    auto start = std::chrono::steady_clock::now();
    auto end_time = start + std::chrono::seconds(10);
    for(int i = 0;i < 20;i++) {
        if (this->current_status == Status::Follower) {
            electionTimeout = getElectionTimeout();
            std::unique_lock<std::mutex> lockM(mutex_message);
            bool message_arrived = cv_incoming_message.wait_for(lockM, electionTimeout, [&] {
                return !incomingMessages.empty();
            });
            lockM.unlock();
            if (message_arrived) {
                processIncomingMessages(nodes);
                this->heartbeatReceived = false;
            } else {
                {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cout << "[Node " << id << "] Follower timed out. Starting election.\n";
                }
                startElection(nodes);
            }
        }else if(this->current_status == Status::Candidate){
            processIncomingMessages(nodes);
        }else if (this->current_status == Status::Leader) {
            auto now = std::chrono::steady_clock::now();
            {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "[Node " << id << "] is Leader. Sending heartbeat. (Interval: " << heartbeatInterval.count() << " ms)\n";
            }
            int num_logs_to_send = 0;
            bool heartbeat_send = false;
            heartbeatInterval = getHeartbeatInterval();
            if (now >= this->nextHeartbeatTime) {
                    heartbeat_send = true;
                    do {
                    nextHeartbeatTime += heartbeatInterval;
                    } while (nextHeartbeatTime <= now + heartbeatInterval);
                }else {
                    std::mt19937 rand_gen(std::random_device{}());
                    std::uniform_int_distribution<int> num_entries_dist(1, 5);
                    num_logs_to_send = num_entries_dist(rand_gen);
                }
                {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cout << "number of logs to send " << num_logs_to_send << " \n";
                }
            AppendEntries log_entries_msg = create_entries(num_logs_to_send,heartbeat_send);
            Message msg;
            msg.type = MessageType::AppendEntries;
            msg.data = log_entries_msg;
            sendAppendEntries(msg,nodes);
            std::unique_lock<std::mutex> lock(mutex_message);
            cv_incoming_message.wait_for(lock, heartbeatInterval, [&] {
                return !message_queue_for_loop.empty();
            });
            lock.unlock();
            processIncomingMessages(nodes);
    }
    }
    return 0;
}
LogEntry RaftNode::deserialize(const std::vector<char>& buffer) {
    LogEntry entry;
    size_t offset = 0;
    std::memcpy(&entry.term, &buffer[offset], sizeof(entry.term));
    offset += sizeof(entry.term);

    std::memcpy(&entry.op, &buffer[offset], sizeof(entry.op));
    offset += sizeof(entry.op);

    uint32_t key_length;
    std::memcpy(&key_length, &buffer[offset], sizeof(key_length));
    offset += sizeof(key_length);
    entry.key.assign(&buffer[offset], key_length);

    uint32_t command_length;
    std::memcpy(&command_length, &buffer[offset], sizeof(command_length));
    offset += sizeof(command_length);
    entry.command.assign(&buffer[offset], command_length);

    return entry;
}
std::vector<LogEntry> RaftNode::recovery_wal(const std::string& walPath){
    std::vector<LogEntry> recoveredEntries;
        std::ifstream walFile(walPath, std::ios::binary);
   if(!walFile.is_open()){
       return recoveredEntries;
   }
   WalRecordHeader header;
   int offset = 0;
   while (walFile.read(reinterpret_cast<char*>(&header), sizeof(header))) {
       std::vector<char> payload(header.payload_len);
        if (!walFile.read(payload.data(), header.payload_len)) {
            break;
        }

   if(crc32(0L, reinterpret_cast<const Bytef*>(payload.data()), payload.size()) != header.crc32){
       return recoveredEntries;
   }
   std::vector<char> buffer(payload.begin(),payload.end());
   recoveredEntries.push_back(deserialize(buffer));
          offset = walFile.tellg();
   }
   return recoveredEntries;
}
HardState RaftNode::recovery_Hard_state(const std::string& hardPath){
    HardState default_state = {0, 0, 0, 0, 0};
    int fd = open(hardPath.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Hard state file not found or could not be opened." << std::endl;
            return default_state;
        }
    struct stat st;
        if (fstat(fd, &st) == -1 || st.st_size != sizeof(HardState)) {
            std::cerr << "Hard state file is incomplete or corrupted." << std::endl;
            close(fd);
            return default_state;
        }
        HardState hard_state;
            if (read(fd, &hard_state, sizeof(HardState)) == -1) {
                std::cerr << "Failed to read hard state file." << std::endl;
                close(fd);
                return default_state;
            }

            close(fd);
int expected_checksum = hard_state.checksum;
hard_state.checksum = 0;
int checksum = crc32(0L, reinterpret_cast<const Bytef*>(&hard_state), sizeof(hard_state) - sizeof(hard_state.checksum));
if(checksum != expected_checksum){
    std::cerr << "Hard state file is corrupted. Checksum mismatch." << std::endl;
    return hard_state = {0,0,0};
}
std::cout << "Successfully recovered hard state." << std::endl;
    return hard_state;
}
LogEntry create_log(int term_input){
    std::string command= "hello";
    std::string key = "ahoj";
    std::mt19937 rand_gen(std::random_device{}());
    std::uniform_int_distribution<int> num(0, 2);
    Op op = static_cast<Op>(num(rand_gen));
    LogEntry new_log{ term_input, op, key, command };
    return new_log;
}
void RaftNode::sendAppendEntries(Message msg,const std::vector<std::unique_ptr<RaftNode>>& nodes){
    {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "[Node " << id << "] Sending Log or heartbeat as Leader. term: " << this->currentTerm << "\n";
    }
    for (auto &node : nodes) {
        if (node.get() != this) {
        {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "[Node " << id << "] -> [Node " << node->id << "] Log send.\n";
        }
        sendMessageToNode(node->id, msg,nodes);
        }
    }
}
AppendEntries RaftNode::create_entries(int num_log,bool heartbeat){
    int term = this->currentTerm;
    int leaderId = this->id;
    const int oldLast = static_cast<int>(this->log.size());
    const int prevLogIndex = oldLast;
    const int prevLogTerm = (oldLast > 0) ? this->log[oldLast - 1].term : 0;
    std::vector<LogEntry> entries;
    entries.reserve(num_log);
    if(!heartbeat){
    for (int i = 0; i < num_log; ++i) {
        LogEntry new_log = create_log(term);
                entries.push_back(new_log);
        }
    int snapshot = 0;
    {
        std::lock_guard<std::mutex> wlock(mutex_wal);
        if(appendWal(entries) == 0){
            snapshot = -1;
        }
    }
    {
        std::lock_guard<std::mutex> wlock(cerr_mutex);
        if(snapshot == -1){
           std::cerr << "error in backup";
        }
    }
    this->log.insert(this->log.end(), entries.begin(), entries.end());
    }else {
        heartbeat = false;
    }
    int leaderCommit = this->commitIndex;
    AppendEntries new_answer {term,leaderId,prevLogIndex,prevLogTerm,entries,leaderCommit};
    return new_answer;
}
std::string RaftNode::messageTypeToString(MessageType type) {
switch (type) {
case MessageType::AppendEntries: return "AppendEntries";
case MessageType::AppendEntriesResponse: return "AppendEntriesResponse";
case MessageType::VoteRequest: return "RequestVote";
case MessageType::VoteResponse: return "VoteResponse";
default: return "Unknown";
}
}
std::vector<char> serializeHardState(uint64_t term, uint64_t vote) {
    HardState state{};
    state.magic = 0xDEADBEEF;
    state.version = 1;
    state.currentTerm = term;
    state.votedFor = vote;
    state.checksum = 0;

      size_t dataSize = sizeof(HardState) - sizeof(state.checksum);
      uint32_t calculatedChecksum = crc32(0L, reinterpret_cast<const Bytef*>(&state), dataSize);
      state.checksum = calculatedChecksum;
    std::vector<char> buffer(sizeof(HardState));
    std::memcpy(buffer.data(), &state, sizeof(HardState));

    return buffer;
}
static bool writeAll(int fd, const void* data, size_t len) {
    const char* p = static_cast<const char*>(data);
    while (len > 0) {
        ssize_t n = ::write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p   += static_cast<size_t>(n);
        len -= static_cast<size_t>(n);
    }
    return true;
}
int RaftNode::saveHardState(){
    if(checkFile() == false){
        return 0;
    }
    std::string path = hard_state_path + ".tpm";
      FILE *file = fopen(path.c_str(),"wb");
      if (!file) {
              return -1;
          }
      std::vector<char> hardstate = serializeHardState(this->currentTerm, this->votedFor);
      size_t bytes_W = fwrite(hardstate.data(), sizeof(char), hardstate.size(),file);
      if (bytes_W != hardstate.size()) {
          fclose(file);
          return -1;
      }
      int fd = fileno(file);
      if(fsync(fd) == -1){
          fclose(file);
                  return -1;
              }
          fclose(file);
    if (rename(path.c_str(), hard_state_path.c_str()) == -1) {
            return -1;
        }
    FILE* dir = fopen(data_dir.c_str(), "r");
        if (dir) {
            fsync(fileno(dir));
            fclose(dir);
        }
       return 0;
}
std::vector<char> serialize(const LogEntry &entry) {
    std::vector<char> buffer;
    size_t total_size = sizeof(entry.term) + entry.command.size() + sizeof(uint32_t) + sizeof(entry.op) + entry.key.size() + sizeof(uint32_t);
    buffer.resize(total_size);

        size_t offset = 0;
        std::memcpy(&buffer[offset], &entry.term, sizeof(entry.term));
        offset += sizeof(entry.term);

        std::memcpy(&buffer[offset], &entry.op, sizeof(entry.op));
        offset += sizeof(entry.op);

        uint32_t key_length = entry.key.size();
        std::memcpy(&buffer[offset], &key_length, sizeof(key_length));
        offset += sizeof(key_length);
        std::memcpy(&buffer[offset], entry.key.data(), entry.key.size());

        uint32_t command_length = entry.command.size();
        std::memcpy(&buffer[offset], &command_length, sizeof(command_length));
        offset += sizeof(command_length);

        std::memcpy(&buffer[offset], entry.command.data(), entry.command.size());
        return buffer;
}
int RaftNode::appendWal(const std::vector<LogEntry> &logs){
    if(checkFile() == false){
        {
            std::lock_guard<std::mutex> Clock(cerr_mutex);
            std::cerr << "error in checkFile \n";
        }
        return 0;
    }
    if (logs.empty()){
     return -1; //hearbeat
    }
    std::vector<char> walS;
    int fd = open(wal_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd == -1) {

            std::cerr << "Error opening file: " << strerror(errno) << std::endl << "\n";
            std::cerr << wal_path.c_str() << "\n";
            return 0;
        }
        bool ok = true;
    for(int i = 0;i < logs.size() && ok;i++){
        std::vector<char> payload = serialize(logs[i]);
        WalRecordHeader hdr;
        hdr.magic       = WAL_MAGIC;
        hdr.version     = WAL_VERSION;
        hdr.payload_len = static_cast<uint32_t>(payload.size());
        hdr.crc32       = crc32(0L,reinterpret_cast<const Bytef*>(payload.data()),static_cast<uInt>(payload.size()));
        hdr.term        = static_cast<uint64_t>(logs[i].term);
        ok = writeAll(fd, &hdr, sizeof(hdr));
                if (ok) ok = writeAll(fd, payload.data(), payload.size());
            }
        if (ok) {
                if (::fsync(fd) == -1) ok = false;
            }else {
                {
                    std::lock_guard<std::mutex> Clock(cerr_mutex);
                    std::cerr << "error in writeAll";
                    return 0;
                }
            }
        close(fd);
    return 1;
}
bool RaftNode::init_from_storage() {
     struct stat st{};
     if (stat("./backup", &st) == -1) {
     if (mkdir("./backup", 0755) == -1 && errno != EEXIST) {
     std::lock_guard<std::mutex> lock(cerr_mutex);
     std::cerr << "[Node " << id << "] Failed to create backup dir: " << strerror(errno) << "\n";
     return false;
     }
     }
     data_dir = "./backup/node-" + std::to_string(id);
     hard_state_path = data_dir + "/hard_state.dat";
     wal_path = data_dir + "/walSave";
     if (stat(data_dir.c_str(), &st) == -1) {
     if (mkdir(data_dir.c_str(), 0755) == -1 && errno != EEXIST) {
     std::lock_guard<std::mutex> lock(cerr_mutex);
     std::cerr << "[Node " << id << "] Failed to create backup dir: " << "./backup" << data_dir.c_str() << strerror(errno) << "\n";
     return false;
     }
     }
     HardState hs = recovery_Hard_state(hard_state_path);
     currentTerm = hs.currentTerm;
     votedFor = hs.votedFor;
     auto recovered = recovery_wal(wal_path);
     log = std::move(recovered);
     commitIndex = 0;
     lastApplied = 0;
     current_status = Status::Follower;
     applyCommitted();
     return true;
     }

     void RaftNode::applyCommitted() {
            const int lastLogIndex = static_cast<int>(log.size());
            if (commitIndex > lastLogIndex) commitIndex = lastLogIndex;
            while (lastApplied < commitIndex) {
                int nextIndex = lastApplied + 1;
                int vecIdx = nextIndex - 1; // 0-based
                if (vecIdx < 0 || vecIdx >= static_cast<int>(log.size())) break;
                const LogEntry& e = log[vecIdx];
                lastApplied = nextIndex;
            }
            }
bool RaftNode::checkFile(){
static std::once_flag init_flag;
std::call_once(init_flag, [&]{
     struct stat st;
    if (stat("./backup", &st) == -1) {
           if (mkdir("./backup", 0755) == -1 && errno != EEXIST) {
               std::lock_guard<std::mutex> lock(cerr_mutex);
               std::cerr << "[Node " << id << "] mkdir(\"./backup\") failed: "
                         << strerror(errno) << std::endl;
               throw std::runtime_error("backup dir create failed");
           }
       }

       data_dir        = std::string("./backup/node-") + std::to_string(id);
       hard_state_path = data_dir + "/hard_state.dat";
       wal_path        = data_dir + "/walSave";

       if (stat(data_dir.c_str(), &st) == -1) {
           if (mkdir(data_dir.c_str(), 0755) == -1 && errno != EEXIST) {
               std::lock_guard<std::mutex> lock(cerr_mutex);
               std::cerr << "[Node " << id << "] mkdir(\"" << data_dir << "\") failed: "
                         << strerror(errno) << std::endl;
               throw std::runtime_error("node dir create failed");
           }
       }
   });
   return true;
}
