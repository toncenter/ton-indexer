// Error while generating alias 'HasUnsupportedMapKey'
// [NonStandardDictKey] 'HasUnsupportedMapKey' is 'map<JustInt32, ...>'
// such a non-standard map key can not be handled by @ton/core library

contract Err {
    incomingMessages: ()
    storage: null
    forceAbiExport: HasUnsupportedMapKey
}

struct JustInt32 { v: int32 }

type HasUnsupportedMapKey = map<JustInt32, address>


fun onInternalMessage() {}
