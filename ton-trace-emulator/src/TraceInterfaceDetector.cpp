#include "TraceInterfaceDetector.h"
#include "smc-interfaces/InterfacesDetector.h"

void TraceInterfaceDetector::start_up() {
    td::MultiPromise mp;
    auto ig = mp.init_guard();
    auto P = td::PromiseCreator::lambda([&, SelfId=actor_id(this)](td::Result<td::Unit> res) mutable {
        td::actor::send_closure(SelfId, &TraceInterfaceDetector::finish, std::move(res));
    });
    ig.add_promise(std::move(P));

    std::queue<std::reference_wrapper<Trace>> queue;
    queue.push(*trace_);

    while (!queue.empty()) {
        Trace& current = queue.front();
        queue.pop();

        for (auto child : current.children) {
            queue.push(*child);
        }
        block::StdAddress address{current.account->workchain, current.account->addr};

        td::actor::create_actor<Trace::Detector>
            ("InterfacesDetector", address, current.account->code, current.account->data, shard_states_, config_, 
            td::PromiseCreator::lambda([SelfId = actor_id(this), &current, promise = ig.get_promise()](std::vector<typename Trace::Detector::DetectedInterface> interfaces) mutable {
                current.interfaces = std::move(interfaces);
                promise.set_value(td::Unit());
        })).release();
    }
}

void TraceInterfaceDetector::finish(td::Result<td::Unit> status) {
    if (status.is_error()) {
        promise_.set_error(status.move_as_error_prefix("Failed to detect interfaces: "));
    } else {
        promise_.set_value(std::move(trace_));
    }
    stop();
}