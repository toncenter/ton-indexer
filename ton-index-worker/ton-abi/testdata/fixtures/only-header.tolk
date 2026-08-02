contract OnlyHeader {
    incomingMessages: InMsgA | InMsgB,
    outgoingMessages: OutMsgA | AliasOutMsgB
    emittedEvents: OutMsgExtA
    thrownErrors: ErrCo
}

// declare types mentioned in `contract`

struct (0x01) InMsgA { fieldA: int32 }
struct (0x02) InMsgB { fieldB: int64 }

struct (0xF0) OutMsgA { fieldA: int32 }
/// desc OutMsgB
struct (0xF1) OutMsgB { fieldB: int64 }

type AliasOutMsgB = OutMsgB

struct OutMsgExtA { value: cell }

enum ErrCo {
    /// desc NotFound
    NotFound = 404,
}


// declare get methods and their types

const FAKE_ADDR = address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")

struct CalcSwapCostReply {
    from: address
    to: address
    cost: coins
}

get fun calc_swap_cost(from: address): CalcSwapCostReply {
    // to satisfy the type system, we need to return something;
    // not very beautiful, but let it be this way for now
    return {
        from: FAKE_ADDR,
        to: FAKE_ADDR,
        cost: 0,
    }
}

get fun calc_owner(owner: address?): address? {
    return null
}

// and an empty entrypoint

fun onInternalMessage() {
}
