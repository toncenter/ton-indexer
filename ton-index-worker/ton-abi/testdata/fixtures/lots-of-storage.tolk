contract LotsOfStorage {
    incomingMessages: ()
    storage: StWithAllDefaults
}

import "@stdlib/lisp-lists"

const ONE = 1
const TWO = ONE + (((ONE * 1)))

struct Inner {
    in1: int32
    in2: bool = true
}

struct StWithAllDefaults {
    i1: int32 = 0
    i2: coins = ton("0.05")
    i3: int8 = ONE
    i4: int32 = TWO * +1
    i5: uint256 = 1 << 100
    i6: int256 = -(1 << 100) + 1 as int256
    i7: int8? = null

    e1: Color = Color.green
    e2: E0Max = E0Max.MAX_INT

    b1: bool = true
    b2: bool = false
    b3: bool = !true

    s1: RemainingBitsAndRefs = "0102".hexToSlice()
    s2: bits8 = "a".literalSlice() as bits8
    s3: bytes2 = stringHexToSlice("0809") as bytes2
    s4: RemainingBitsAndRefs = "hello1#@".literalSlice()
    s5: string = "kopi"

    a1: address = address("0:ca6e321c7cce9ecedf0a8ca2492ec8592494aa5fb5ce0387dff96ef6af982a3e")
    a2: address = address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")
    a3: address? = null
    a4: any_address = address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c") as any_address
    a5: any_address? = null

    t1: (int8, int8, int8)? = (1, 2, 3)
    t2: (RemainingBitsAndRefs, address, coins) = ("".hexToSlice(), address("9:527964d55cfa6eb731f4bfc07e9d025098097ef8505519e853986279bd8400d8"), 1000000)
    t3: (int8?, (int8, int8)) = (null, (20, 30))
    t4: (uint32, uint16, uint256, uint32, int256) = (stringCrc32("hello") | 0, stringCrc16("hello") & 0xFFFFFFFF, stringSha256("hello"), stringSha256_32("hello"), stringToBase256("hello"))
    t5: (int8, int8)? = null

    sh1: [int8, bool?] = [1, null]
    sh2: [(int8, bool?), [string]] = [(1, null), ["10"]]

    arr1: (array<int32>, lisp_list<int32>) = ([1, 2, 3], [4, 5, 6]);

    o1: Inner = { in1: 1, in2: false }
    o2: Inner? = { in1: 2 }
    o3: Inner? = null
}

enum Color {
    red,
    green,
    blue,
}

enum E0Max {
    ZERO,
    MAX_INT = +115792089237316195423570985008687907853269984665640564039457584007913129639935,
}

fun onInternalMessage() {
    StWithAllDefaults{}.toCell()
}

