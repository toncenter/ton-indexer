contract LotsOfAnnotations {
    author: "A K",
    version: """1.0""",
    description: "some d",
    custom: {
        k: 1,
        kk: 2,
    }
    incomingMessages: Msg1 | ResetTo<int32> | Transfer
    storage: MyStorage
    incomingExternal: ActualExternalShape
} 

/// mmm1
/// mmm2
struct (0x12345678) Msg1 {
    
}

/// mmmReset
struct (0x23456789) ResetTo<T> {
    counter_id: int32
    reset_to: T = 0
}

struct (0x3456789A) Transfer {
    owner: address
    /// actually it's not a slice
    forwardPayload: RemainingBitsAndRefs
}

struct Out2 {
    v2: int8
}

struct Out3<PayloadT> {
    payload: PayloadT
}

/// mmmOut4
struct OutExt4 {
    v4: int8
}

struct MyStorage {
    collectionAddress: address
    content: cell
}

/// mmmShape
struct ActualExternalShape {
    v: int32
    sign: bits512
}

fun onInternalMessage(in: InMessage) {
    match (in.createdAt) {
        0 => {
            createMessage({
                bounce: BounceMode.Only256BitsOfBody,
                value: 0,
                dest: contract.getAddress(),
            })
        }
        10 => {
            createMessage({
                bounce: BounceMode.NoBounce,
                value: 0,
                dest: contract.getAddress(),
                body: 123 as int8,
            })
        }
        20 => {
            createMessage({
                bounce: BounceMode.NoBounce,
                value: 0,
                dest: contract.getAddress(),
                body: Transfer {
                    owner: contract.getAddress(),
                    forwardPayload: createEmptySlice(),
                },
            })
        }
        30 => {
            createMessage({
                bounce: BounceMode.RichBounce,
                value: 0,
                dest: contract.getAddress(),
                body: Out2 {
                    v2: 19,
                }.toCell(),
            })
        }
        40 => {
            createMessage({
                bounce: BounceMode.NoBounce,
                value: 0,
                dest: contract.getAddress(),
                body: Out3<(int8, int16)> {
                    payload: (1, 2),
                },
            })
        }
        50 => {
            createExternalLogMessage({
                dest: createAddressNone(),
                body: UnsafeBodyNoRef {
                    forceInline: OutExt4 {
                        v4: 30,
                    },
                },
            })
        }
    }
}

fun onExternalMessage(inMsg: slice) {
    
}

/// get1
/// @param spec some number
get fun getFirst(spec: int = 50) {
    return 1
}
