
#include "../include/raft_node.hpp"
#include <mutex>
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
    while (true) {
               std::string input = inputQueue.pop();
               if (input == "exit") {
                   break;
               }
               ClientRequest newRequest = {};
               newRequest.key = input;
               std::mt19937 rand_gen(std::random_device{}());
               std::uniform_int_distribution<int> num(0, 2);
               Op op = static_cast<Op>(num(rand_gen));
               newRequest.op = op;
               newRequest.id = this->id;
               int target_node_id = this->id;
               if(nodes[target_node_id] -> current_status != Leader){
                   target_node_id = (this->id + 1) % nodes.size();
               }
              Message msg;
              msg.type = MessageType::ClientRequest;
              msg.data = newRequest;
              while (message_sent == false) {


              sendMessageToNode(target_node_id, msg,nodes);
              std::unique_lock<std::mutex> lock(mutex_message);
              cv_incoming_message.wait(lock);
              lock.unlock();
              processIncomingMessages(nodes);


              }
           }
       }
