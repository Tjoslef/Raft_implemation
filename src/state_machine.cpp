
#include "../include/raft_node.hpp"
#include <unistd.h>
#include <vector>

void StateMachine::apply(std::vector<LogEntry> &logs) {
    for(auto &log:logs){
        switch (log.op) {
        case Op::Put: kv[log.key] = log.command; break;
        case Op::Del: kv.erase(log.key); break;
        case Op::NoOp: default: break;
        }
    }
}
