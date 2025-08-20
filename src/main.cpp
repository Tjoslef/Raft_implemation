#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include "../include/raft_node.hpp"
int main(){
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<std::thread> threads;
        const int clusterSize = 7;
        nodes.reserve(clusterSize);
    for (int i = 0;i < clusterSize;i++) {
        nodes.push_back(std::make_unique<RaftNode>(i));
        nodes[i]->init_from_storage();
    }
    ThreadSafeQueue<std::string> clientInputQueue;
    auto raftNode = std::make_unique<RaftNode>();
    raftNode->id = 0;
    //waiter
    std::thread client_thread(&RaftNode::sendClient,nodes[0].get(),std::ref(clientInputQueue), std::ref(nodes));
    //thread for user input
    std::thread input_thread(&RaftNode::commands,nodes[1].get(),std::ref(clientInputQueue));
    auto start_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    for(auto &node:nodes){
        threads.emplace_back([nodePtr = node.get(), &nodes, start_time]() {
            std::this_thread::sleep_until(start_time);
            if (int feedback = nodePtr->Loop(nodes); feedback != 0) {
                            std::cerr << "Error in thread " << std::this_thread::get_id()
                                      << ": node returned error (" << feedback << ").\n";
                        }
    });

    }
    //waiting
    for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    client_thread.join();
        return 0;
    }
