tolk 1.4

struct Node {
    addr: any_address;
    next: Cell<Node> | void;
}

struct (0x7258a69b) VestingAddWhiteList {
    query_id: uint64;
    head: Node;
}

type AllowedMessage = VestingAddWhiteList

contract VestingMessages {
    incomingMessages: AllowedMessage
}
