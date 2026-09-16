import "@stdlib/tvm-lowlevel"

// Error while generating get method 'returningContinuation'
// 'result[1]' can not be used in get methods, because it contains 'continuation'

contract Err {
    incomingMessages: ()
    storage: null
}

get fun returningContinuation() {
    return (1, transformSliceToContinuation(createEmptySlice()))
}


fun onInternalMessage() {}
