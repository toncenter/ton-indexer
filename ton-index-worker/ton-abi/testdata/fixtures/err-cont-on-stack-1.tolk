// Error while generating get method 'acceptingContinuation'
// 'd.n' can not be used in get methods, because it contains 'continuation'

contract Err {
    incomingMessages: ()
    storage: null
}

struct D {
    n: (int) -> void
}

get fun acceptingContinuation(d: D) {
    return 123;
}


fun onInternalMessage() {}
