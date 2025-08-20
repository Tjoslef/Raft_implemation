
#include "../include/raft_node.hpp"
#include <iostream>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <vector>

void StateMachine::apply(std::vector<LogEntry> &logs) {
    for(auto &log:logs){
        switch (log.op) {
        case Op::Put: kv[log.key] = log.key; break;
        case Op::Del: kv.erase(log.key); break;
        case Op::NoOp: default: break;
        }
    }
}
void RaftNode::sendClient(ThreadSafeQueue<std::string>& inputQueue,const std::vector<std::unique_ptr<RaftNode>>& nodes){
    auto start_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(10000);
    std::this_thread::sleep_until(start_time);
    while (true) {
               std::string input = inputQueue.pop();
               if (input == "exit") {
                   {
                       std:std::lock_guard<std::mutex>lock(cout_mutex);
                       std::cout << "ending connection";
                   }
                   break;
               }
               int correlationId = nextRequestId.fetch_add(1);
               ClientRequest newRequest = {};
               newRequest.key = input;
               std::mt19937 rand_gen(std::random_device{}());
               std::uniform_int_distribution<int> num(0, 2);
               Op op = static_cast<Op>(num(rand_gen));
               newRequest.op = op;
               newRequest.id = this->id;
               newRequest.correlationId = correlationId;
               int target_node_id;
               {
                   std::lock_guard<std::mutex> lg(mutex_election);
                   target_node_id = this->lastLeader;
               }
              Message msg;
              msg.type = MessageType::ClientRequest;
              msg.data = newRequest;
              auto waiter = std::make_shared<CommitWaiter>();

              {
                  std::lock_guard<std::mutex> lg(waiters_mutex);
                  waiters[correlationId] = waiter;
              }
              sendMessageToNode(target_node_id, msg,nodes);
              std::unique_lock<std::mutex> lock(waiter->mtx);
              waiter->cv.wait(lock, [&]{ return waiter->committed; });
           }
       }
void RaftNode::notifyWaiter(int correlation_id){
    std::shared_ptr<CommitWaiter> w;
    int snapshot_commit = 0;
    {
        std::lock_guard<std::mutex> lock(waiters_mutex);
        auto x = waiters.find(correlation_id);
        if(x != waiters.end()){
            w = x->second;
            waiters.erase(x);
        }
    }
    if(w){
        {
        std::lock_guard<std::mutex> lk(w->mtx);
               w->committed = true;
               snapshot_commit = 1;
        }
    if(snapshot_commit == 1){
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "we have commit \n";
    }
    w->cv.notify_one();
    }
}
void RaftNode::commands(ThreadSafeQueue<std::string>& inputQueue){
auto start_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(10000);
std::string input;
std::lock_guard<std::mutex> lock(cout_mutex);
std::cout << "Enter commands to send to the Raft node (type 'exit' to quit):" << std::endl;
while (std::getline(std::cin, input) && input != "exit") {
    clientInputQueue.push(input);
}
clientInputQueue.push("exit");
std::cout << "ending connection";
}
