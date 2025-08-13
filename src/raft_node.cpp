#include "../include/raft_node.hpp"
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <variant>
std::mutex cerr_mutex;
std::mutex cout_mutex;
RaftNode::RaftNode(int nodeId)
    : id(nodeId),
      rand_gen(rd()),
      electionTimeoutDistribution(300,600 ),
      heartbeatIntervalDistribution(100, 150)
{
    this->electionTimeout = getElectionTimeout();
    this->heartbeatInterval = getHeartbeatInterval();
    std::cout << "RaftNode " << id << " created. ElectionTimeout: " << electionTimeout.count() << " ms, HeartbeatInterval: " << heartbeatInterval.count() << " ms\n";
}

void RaftNode::checkAndCommitLogs(const std::vector<std::unique_ptr<RaftNode>>& nodes) {
    if (this->current_status != Status::Leader) return;
    std::vector<int> sortedMatchIndexes;
    for (size_t i = 0; i < nodes.size(); ++i) {
        sortedMatchIndexes.push_back(this->matchIndex[i]);
    }
    std::sort(sortedMatchIndexes.begin(), sortedMatchIndexes.end());

    int N = nodes.size();
    int majorityIndex = sortedMatchIndexes[N / 2];

    if (majorityIndex > this->commitIndex && this->log[majorityIndex - 1].term == this->currentTerm) {
        this->commitIndex = majorityIndex;
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
                        // Apply Raft log conflict resolution: truncate on first mismatch, then append
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
                        // Advance commitIndex
                        if (temp->leaderCommit > commitIndex) {
                            commitIndex = std::min(temp->leaderCommit, (int)log.size());
                        }
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
                          << (voteRes.voteGranted ? "true" : "false")
                          << " counted=" << (counted ? "true" : "false")
                          << " votes=" << votes_snapshot
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
            // Unknown/unhandled type
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
    }
    node->cv_incoming_message.notify_one();
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
    {
        std::lock_guard<std::mutex> lock(mutex_election);
        this->current_status = Status::Candidate;
        votesGranted++;
        this->currentTerm++;
        this->votedFor = id;
    }
    {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "[Node " << id << "] Starting election for term " << this->currentTerm
              << ", votesGranted = " << votesGranted << "\n";
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
    bool won;
    auto deadline = std::chrono::steady_clock::now() + electionTimeout;
    const size_t majority = nodes.size() / 2 + 1;
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
        nextHeartbeatTime = std::chrono::steady_clock::now();

        lock.unlock();

        {
            std::lock_guard<std::mutex> lk(cout_mutex);
            std::cout << "[Node " << id << "] Became Leader for term " << currentTerm
                      << " with votes=" << votesGranted << "\n";
        }
    } else {
        Status status_snapshot = current_status;
        int votes_snapshot = votesGranted;
        lock.unlock();
        {
            std::lock_guard<std::mutex> lk(cout_mutex);
            std::cout << "[Node " << id << "] Election not won (status=" << (int)status_snapshot
                      << ", votes=" << votes_snapshot
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
                { std::lock_guard<std::mutex> lock(cout_mutex);
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
                    nextHeartbeatTime = now + heartbeatInterval;
                }else {
                    std::mt19937 rand_gen(std::random_device{}());
                    std::uniform_int_distribution<int> num_entries_dist(1, 5);
                    num_logs_to_send = num_entries_dist(rand_gen);
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

            processIncomingMessages(nodes);
    }
    }
    return 0;
}
LogEntry create_log(int term_input){
    std::string command= "hello";
    LogEntry new_log{ term_input,command};
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
    std::vector<LogEntry> entries;
    entries.reserve(num_log);
    if(!heartbeat){
    for (int i = 0; i < num_log; ++i) {
        LogEntry new_log = create_log(term);
                entries.push_back(new_log);
                this->log.push_back(new_log);
        }
    }
    int prevLogIndex= this->lastLogIndex - num_log;
    int prevLogTerm = (lastLogIndex > 0) ? this->log[lastLogIndex-1].term:0;
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
