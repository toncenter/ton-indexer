contract GenericUnionLabels {
    incomingMessages: IncomingMessage
    storage: GenericUnionLabelsStorage
}

struct Point {
    x: int8
    y: int8
}

type MyNullable<T> = T?

struct GenericUnionLabelsStorage {
    dummy: uint32
}

struct GenericPair<T1, T2> {
    value: T1 | T2
}

struct GenericOrInt8<T> {
    value: T | int8
}

type AliasOrInt8<T> = T | int8
type RawEither<X, Y> = X | Y

struct (0x10000001) MsgPair {
    value: GenericPair<int32, int64>
}

struct (0x10000003) MsgOrInt16 {
    value: GenericOrInt8<int16>
}

struct (0x10000005) MsgAliasInt16 {
    value: AliasOrInt8<int16>
}

type IncomingMessage =
    MsgPair |
    MsgOrInt16 |
    MsgAliasInt16

struct (0b0) TlbIntMsgInfoRelaxed {
    bounce: bool
    dest: address
    value: coins
}

struct TlbMessageRelaxedGeneric {
    info: TlbIntMsgInfoRelaxed
    init: TlbMaybe<TlbEither<StateInit, Cell<StateInit>>>
    body: RemainingBitsAndRefs
}

type TlbEither<X, Y> = TlbEitherLeft<X> | TlbEitherRight<Y>

struct (0b0) TlbEitherLeft<X> {
    value: X
}

struct (0b1) TlbEitherRight<X> {
    value: X
}

type TlbMaybe<X> = TlbNone | TlbJust<X>

struct (0b0) TlbNone

struct (0b1) TlbJust<X> {
    value: X
}

fun onInternalMessage() {}

get fun getPair(): GenericPair<int32, int64> {
    return { value: 10 as int32 }
}

get fun getOrInt16(): GenericOrInt8<int16> {
    return { value: 12 as int16 }
}

get fun getAliasInt16(): AliasOrInt8<int16> {
    return 14 as int16
}

get fun idMyNullablePoint(p: MyNullable<Point>): (MyNullable<Point>, Cell<MyNullable<Point>>) {
    return (p, p.toCell())
}

get fun getRelaxedMessage(): TlbMessageRelaxedGeneric {
    return {
        info: {
            bounce: true,
            dest: address("0:527964d55cfa6eb731f4bfc07e9d025098097ef8505519e853986279bd8400d8"),
            value: ton("0.05"),
        },
        init: TlbNone {},
        body: createEmptySlice(),
    }
}

get fun checkRelaxedMessage(msg: TlbMessageRelaxedGeneric): int {
    assert (msg.info.bounce) throw 101;
    assert (msg.info.value == ton("0.05")) throw 102;
    assert (msg.body.isEmpty()) throw 103;
    return 777
}

get fun getEitherInt32Bool(): TlbEither<int32, bool> {
    return TlbEitherLeft { value: 19 as int32 }
}

get fun getEitherBoolBool(): TlbEither<bool, bool> {
    return TlbEitherLeft { value: true }
}

get fun getRawEitherInt32Bool(): RawEither<int32, bool> {
    return 20 as int32
}
