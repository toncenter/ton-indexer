// Error while generating struct 'Demo'
// [NonStandardDictKey] 'Demo.errM' is 'map<Point, ...>'
// such a non-standard map key can not be handled by @ton/core library

contract Err {
    incomingMessages: ()
    storage: null
    forceAbiExport: Demo
}

struct Point {
    x: int8
    y: int8
}

struct Demo {
    a: int8
    errM: map<Point, Point>
}


fun onInternalMessage() {}
