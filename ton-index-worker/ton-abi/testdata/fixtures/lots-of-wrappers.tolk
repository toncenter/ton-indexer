contract LotsOfWrappers {
    incomingMessages: ()
    storage: null
    // this file does not contain get methods, createMessage, etc.
    // that's why in order to export symbols for serialization, force them all
    forceAbiExport: (
        JustUint5,
        JustInt32,
        JustMaybeInt32,
        TwoInts32AndCoins,
        TwoInts32And64,
        TwoInts32AndRef64,
        TwoInts32AndMaybe64,
        JustAddress,
        TwoInts32And64SepByAddress,
        IntAndEitherInt8Or256,
        IntAndEither32OrRef64,
        IntAndEither8OrMaybe256,
        IntAndMaybeMaybe8,
        SomeBytesFields,
        IntAndRestInlineCell,
        IntAndRestRefCell,
        IntAndRestEitherCellOrRefCell,
        DifferentMaybeRefs,
        DifferentIntsWithMaybe,
        DifferentMix1,
        DifferentMix2,
        DifferentMix3,
        WithVariadicInts,
        EdgeCaseInts,
        WriteWithBuilder,
        WriteWithSlice,
        ReadWrittenWithBuilder,
        ReadWriteRest<()>,
        Tail224,
        ReadWriteMid<()>,
        WithTwoRestFields,
        WithMoreTrickyCells,
        WithMoreTrickyAddresses1,
        WithMoreTrickyAddresses2,
        WithAnyAddress,
        MsgSinglePrefix32,
        MsgSinglePrefix48,
        CounterIncrementAlias,
        MsgTransfer,
        ReadRest_Remaining,
        MyInt32,
        MaybeJustOfAddress,
        AddressAlias,
        MsgCounter1,
        BodyPayload,
        MsgExternal1,
        TransferParams,
        Union_8_16_32,
        Union_8_16_32_n,
        WithEnums,
        Point,
        Test4_8,
        Test4_16,
        Test4_32,
        StorWithStr,
        PointWithCustomInt,
        WithMyBorder,
        WithFakeWriter,
        WithEnumsUnion,
        UnionStructs_8_16_32,
        UnionStructs_8_16_32_n,
        U105,
        U106,
        TelegramString,
        Custom8,
        MyBorderedInt,
        MyCustomNothing,
        Tensor3Skipping1,
        TwoRoles,
        Color,
        EFits2Bits,
        EStartFrom1,
        EStartFromM2,
        EFits8Bits,
        EMinMax,
        E0Max,
        Role,
        EncodedVari,
        ECollisionNames,
        WithMaps0,
        WithNullableMaps,
        WithMoreTrickyTypes,
        WithRef,
        ArrayAlias<()>,
        WithArrays,
        WithArrays2,
        WithArrays4,
        LotsOfCoins,
        EmptyE,
        ArrayOfLotsCoins,
        EmptyShape,
        MyShape1,
        Recurrent,
        WithLispLists1,
        WithLispLists2,
        CustomPoint,
        CustomPointAlias,
        WithStrings1,
        WithStrings2,
        `invalid-js-naming%`,
        NotPackable1,
        NotPackable3,
        NotPackable5,
        NotPackable6,
        NotPackable7,
        NotPackable8,
        NotUnpackable1,
        WithNotPackable,
        NotPackable3Alias,
        NotPackable4Alias,
        OnlyWithPack,
        OnlyWithUnpack,
        HasOnlyWithUnpack,
        PrefixedOrVoid,
        PlainOrVoid,
        Int32OrVoid,
        ThreeWayWithVoid,
        InsideStructWithVoid,
        Int32OrNullOrVoid,
    )
}

struct JustUint5 {
    value: uint5
}

struct JustInt32 {
    value: int32
}

struct JustMaybeInt32 {
    value: int32? = null
}

struct TwoInts32AndCoins {
    op: int32
    amount: coins = ton("1")
}

struct TwoInts32And64 {
    op: int32
    query_id: uint64
}

struct TwoInts32AndRef64 {
    op: int32
    query_id_ref: Cell<uint64>
}

struct TwoInts32AndMaybe64 {
    op: int32
    query_id: uint64?
    demo_bool_field: bool
}

struct JustAddress {
    addr: address
}

struct TwoInts32And64SepByAddress {
    op: int32
    addr_e: any_address
    query_id: uint64
}

struct IntAndEitherInt8Or256 {
    op: int32
    i8or256: int8 | int256
}

struct Inner1 {
    query_id_ref: uint64
}

struct Inner2 {
    i64_in_ref: int64
}

struct IntAndEither32OrRef64 {
    op: int32
    i32orRef: int32 | Cell<Inner2>
    query_id_maybe_ref: Cell<Inner1>?
}

struct EitherLeft<T> {
    value: T
}

struct EitherRight<T> {
    value: T
}

struct IntAndEither8OrMaybe256 {
    value: Either<int8, int256?>
    op: int32
}

struct MaybeNothing

struct MaybeJust<T> {
    value: T
}

struct IntAndMaybeMaybe8 {
    value: Maybe<Maybe<int8>>
    op: int32
}

struct SomeBytesFields {
    f1: bits8
    f2: bits3
    f3: bits20?
    f4: bits100 | bits200
}

struct IntAndRestInlineCell {
    op: int32
    rest: RemainingBitsAndRefs
}

struct IntAndRestRefCell {
    op: int32
    rest: cell
}

struct IntAndRestEitherCellOrRefCell {
    op: int32
    rest: RemainingBitsAndRefs | cell
}

struct DifferentMaybeRefs {
    op: int32 = 123
    ref1m: cell?
    ref2m: dict
    ref3: cell
    ref4m32: Cell<JustInt32>?
    query_id: int64
}

struct DifferentIntsWithMaybe {
    ji: JustInt32
    jmi: JustMaybeInt32
    jiMaybe: JustInt32?
    jmiMaybe: JustMaybeInt32?
}

struct DifferentMix1 {
    ja1: JustAddress
    ja2m: JustAddress?
    ext_nn: any_address
    imm: IntAndMaybeMaybe8
    tis: TwoInts32And64SepByAddress
}

struct DifferentMix2 {
    iae: Cell<IntAndEither32OrRef64>
    tic: TwoInts32AndCoins
    rest: RemainingBitsAndRefs
}

struct DifferentMix3 {
    bod: Cell<TwoInts32AndCoins> | Cell<JustInt32>
    tim: TwoInts32AndCoins?
    pairm: (int32, int64)? = null
}

struct WithVariadicInts {
    ui16: varuint16
    i16: varint16
    ui32: varuint32
    i32: varint32
}

struct EdgeCaseInts {
    maxUint: uint256 = 115792089237316195423570985008687907853269984665640564039457584007913129639935
    maxInt: int257 =   115792089237316195423570985008687907853269984665640564039457584007913129639935
    minInt: int257 =  -115792089237316195423570985008687907853269984665640564039457584007913129639935 - 1
}

struct WriteWithBuilder {
    f1: int32
    rest: builder
}

struct WriteWithSlice {
    f1: int32
    rest: slice
}

struct ReadWrittenWithBuilder {
    f1: int32
    someInt: uint32
    someCell: cell?
}

struct ReadWriteRest<T> {
    f1: int32
    f2: coins
    rest: T
}

struct Tail224 {
    ji: JustInt32
    addr: address
    ref1: Cell<int32>?
    ref2: Cell<JustAddress>
}

struct ReadWriteMid<T> {
    f1: int32
    mid: T
    f3: coins
}

struct WithTwoRestFields {
    i32: int32
    rest1: RemainingBitsAndRefs
    rest2: RemainingBitsAndRefs
}

struct MoreTrickyCells<T3, T4> {
    c1: Cell<int32 | int64>
    c2: Cell<Cell<any_address>>
    c3: Cell<T3>?
    c4: Cell<T4> | bool
}

struct WithMoreTrickyCells {
    before: int8
    tricky: MoreTrickyCells<TwoInts32And64, MaybeJust<int8> | int16 | MaybeNothing>
    after: int8
}

@overflow1023_policy("suppress")
struct WithMoreTrickyAddresses1 {
    a1: AddressAlias?
    a2: address | MyInt32 | MaybeNothing
    a3: TwoInts32And64 | MaybeJustOfAddress
    a4: Cell<address?>?
    a5: address | Cell<any_address>
}

@overflow1023_policy("suppress")
struct WithMoreTrickyAddresses2 {
    a1: Maybe<address>
    a2: Maybe<address?>
    a3: JustAddress?
    a4: address | int64 | null
    a5: address | Cell<address>
}

@overflow1023_policy("suppress")
struct WithAnyAddress {
    a1: any_address
    a2: any_address?
    a3: any_address | int64
    a4: address | any_address
    a5: Maybe<any_address>
    a6: Cell<any_address>
    a7: Cell<any_address?>?
}

struct (0x87654321) MsgSinglePrefix32 {
    amount1: coins
    amount2: coins
}

struct (0x876543211234) MsgSinglePrefix48 {
    amount: coins | uint64
}

struct (0x12345678) CounterIncrement {
    counter_id: int8
    inc_by: int32
}

struct (0x23456789) CounterDecrement {
    counter_id: int8
    dec_by: int32
}

struct (0x34567890) CounterReset0 {
    counter_id: int8
}

struct (0x00184300) CounterResetTo {
    counter_id: int8
    initial_value: int64
}

type CounterIncrementAlias = CounterIncrement

struct (0b001) BodyPayload1 {
    should_forward: bool
    n_times: int32
    content: RemainingBitsAndRefs
}

struct (0b01) BodyPayload2 {
    master_id: int8
    owner_address: address
}

struct (0x89) SayHiAndGoodbye {
    dest_addr: address?
    body: BodyPayload
}

struct (0x0013) SayStoreInChain {
    in_masterchain: bool
    contents: Cell<BodyPayload>
}

struct (0x794) TransferParams1 {
    dest_int: address
    amount: coins
    dest_ext: any_address
}

struct (0x9) TransferParams2 {
    intVector: (int32, coins?, uint64)
    needs_more: Cell<bool>
}

struct (0xFB3701FF) MsgTransfer {
    params: Either<TransferParams, Cell<TransferParams>>
}

type ReadRest_Remaining = ReadWriteRest<RemainingBitsAndRefs>

type MyInt32 = int32

type MaybeJustOfAddress = MaybeJust<address>

type AddressAlias = address

type Either<L, R> = EitherLeft<L> | EitherRight<R>

type Maybe<T> = MaybeNothing | MaybeJust<T>

type MsgCounter1 = CounterIncrementAlias | CounterDecrement | CounterReset0 | CounterResetTo

type BodyPayload = BodyPayload1 | BodyPayload2

type MsgExternal1 = SayHiAndGoodbye | SayStoreInChain

type TransferParams = TransferParams1 | TransferParams2

type Union_8_16_32 = int8 | int16 | int32

type Union_8_16_32_n = int8 | int16 | int32 | null

enum EStoredAsInt8 {
    M100 = -100,
    Z = 0,
    P100 = 100,
}

enum EStoredAsUint1 {
    ZERO = 0,
    ONE = 1,
}

struct WithEnums {
    e1: EStoredAsInt8
    e2: EStoredAsUint1
    rem: uint7
}

struct Point {
    x: int8 = 10
    y: int8 = 20
}

struct Test4_8 {
    a: int8
}

struct Test4_16 {
    a: int16
}

struct Test4_32 {
    a: int32
}

struct StorWithStr {
    a: int32
    str: TelegramString
    b: int32
}

struct PointWithCustomInt {
    a: Custom8
    b: int8
}

struct WithMyBorder {
    a: int8
    b: MyBorderedInt
}

struct WithFakeWriter {
    a: int8
    fake: MyCustomNothing
    b: int8
}

struct WithEnumsUnion {
    u: EFits8Bits | EStartFromM2 | null
}

type UnionStructs_8_16_32 = int8 | Test4_16 | Test4_32

type UnionStructs_8_16_32_n = Test4_8 | int16 | int32 | null

type U105 = int8 | int32 | int64

type U106 = int8 | int16 | int32 | int64

type TelegramString = slice

type Custom8 = uint8

type MyBorderedInt = int257

type MyCustomNothing = ()

type Tensor3Skipping1 = (uint8, uint8, uint8)

type TwoRoles = (Role, Role)

enum Color {
    Red = 0,
    Green = 1,
    Blue = 2,
}

enum EFits2Bits {
    ZERO = 0,
    ONE = 1,
    TWO = 2,
}

enum EStartFrom1 {
    ONE = 1,
    TWO = 2,
    THREE = 3,
}

enum EStartFromM2 {
    M2 = -2,
    M1 = -1,
    ZERO = 0,
    P1 = 1,
    P2 = 2,
    P3 = 3,
}

enum EFits8Bits {
    E0 = 0,
    E110 = 110,
    E220 = 220,
}

enum EMinMax {
    MIN_INT = -115792089237316195423570985008687907853269984665640564039457584007913129639935 - 1,
    MAX_INT = 115792089237316195423570985008687907853269984665640564039457584007913129639935,
}

enum E0Max {
    ZERO = 0,
    MAX_INT = 115792089237316195423570985008687907853269984665640564039457584007913129639935,
}

enum Role : int8 {
    Admin = 0,
    User = 1,
}

enum EncodedVari : varint16 {
    ONE = 1,
    TWO = 2,
    MANY = 1267650600228229401496703205376,
}

enum ECollisionNames {
    fromSlice,
    store,
    toCell,
    ToCell,
}

struct Wrapper<T> {
    item: T
}

struct WithMaps0 {
    m1: map<int16, int32>
    m2: map<int16, (int8, int8)>
    m3: map<uint16, any_address>
    m4: map<address, RemainingBitsAndRefs>
}

struct WithNullableMaps {
    m1: map<int32, int64>?
    m2: map<int32, int64>?
    m3: map<int32, int64>?
    m4: map<int32, int64> | int32
    m5: map<int8, int8> | map<int16, int16> | map<int32, int32>
}

struct WithMoreTrickyTypes {
    uni1: (int8, int16) | (int8, int16, int32)
    r1: Cell<Wrapper<int8>>
    r2: Cell<any_address | Wrapper<any_address>>
    nullable1: CounterIncrementAlias?
    nestedM: map<int8, map<uint16, JustInt32>>
    uni2: Wrapper<int8> | Wrapper<int16>
}

struct WithRef {
    id: int32?
    ref: Cell<address>?
}

type ArrayAlias<T> = array<T>

struct WithArrays {
    a1: array<int8>
    a2: array<Cell<int8>>
    a3: array<RemainingBitsAndRefs>
    after: bool = true
}

struct WithArrays2 {
    a: array<array<int8>>
}

struct WithArrays4 {
    before: int32?
    a0: array<Point>
    a1: array<WithRef>
    a2: array<bool?>?
    after: int32
}

@overflow1023_policy("suppress")
struct LotsOfCoins {
    nums: (coins, coins, coins, coins, coins, coins, coins, coins, coins, coins)
}

struct (0x1234) EmptyE

type ArrayOfLotsCoins = array<LotsOfCoins>

type EmptyShape = []
type MyShape1 = [int32?, Point, EmptyShape, EmptyE, Cell<[]>?, int8]

struct Recurrent {
    value: int16
    nextSh: [int4, Cell<Recurrent>?]
}

struct WithLispLists1 {
    lInt8_1: lisp_list<int8>
    lInt8_2: lisp_list<int8>
    lPoint_1: lisp_list<Point>
    lPoint_2: lisp_list<Point>
}

struct WithLispLists2 {
    before: int8
    lInt8: lisp_list<int8>
    lInt32: lisp_list<int32>
    lInt256: lisp_list<int256>
    after: int8
}

struct CustomPoint {
    x: int
    y: int
}

fun CustomPoint.packToBuilder(self, mutate b: builder) {
    b.storeUint(self.x, 8);
    b.storeUint(self.y, 8);
}

fun CustomPoint.unpackFromSlice(mutate s: slice): CustomPoint {
    return {
        x: s.loadUint(8),
        y: s.loadUint(8),
    }
}

type CustomPointAlias = CustomPoint

struct WithStrings1 {
    s1: string
    s2: string?
}

struct WithStrings2 {
    ref: Cell<string>
    refo: Cell<string?>?
    arrs: array<string> = []
}

struct `invalid-js-naming%` {
    `foo-bar`: int32
    `rule()`: int32
    `A^@'"B`: int32
    `void`: void
}

struct NotPackable1 {
    n: int8
    a: (int8, int)
}

struct NotPackable3 {
    t: tuple
}

struct NotPackable5 {
    t: [int8, builder]
}

struct NotPackable6 {
    c: continuation
}

struct NotPackable7 {
    f: (int) -> void
}

struct NotPackable8 {
    u: unknown
}

struct NotUnpackable1 {
    e: (int8, (int8, builder))
}

struct WithNotPackable {
    k: int8
    nestedNot: NotPackable3
}

type NotPackable3Alias = NotPackable3
type NotPackable4Alias = (int8, int16, int)

// unions with a trailing `void` variant ("maybe empty slice")
struct (0x01) PrefixedSome { x: int32 }
type PrefixedOrVoid = PrefixedSome | void

struct PlainSome { x: int32 }
type PlainOrVoid = PlainSome | void

type Int32OrVoid = int32 | void

struct (0b00) ThreeP1 { v: int8 }
struct (0b01) ThreeP2 { v: int16 }
type ThreeWayWithVoid = ThreeP1 | ThreeP2 | void

struct InsideStructWithVoid {
    a: int8
    tail: PrefixedOrVoid
}

type Int32OrNullOrVoid = int32 | null | void


///

fun TelegramString.packToBuilder(self, mutate b: builder) {
    val bytes = self.remainingBitsCount() / 8;
    b.storeUint(bytes, 8);
    b.storeSlice(self);
}

fun TelegramString.unpackFromSlice(mutate s: slice) {
    val bytes = s.loadUint(8);
    return s.loadBits(bytes * 8);
}

fun Custom8.packToBuilder(self, mutate b: builder) {
    b.storeUint(self, 8)
}

fun Custom8.unpackFromSlice(mutate s: slice) {
    return s.loadUint(8) as uint8
}


fun MyBorderedInt.packToBuilder(self, mutate b: builder) {
    if (self > 10) { b.storeUint(1, 4) }
    else if (self > 0) { b.storeUint(2, 4) }
    else { b.storeUint(3, 4) }
}

fun MyBorderedInt.unpackFromSlice(mutate s: slice): MyBorderedInt {
    return match (s.loadUint(4)) {
        1 => 10,
        2 => 0,
        3 => -1,
        else => throw 123
    }
}

fun MyCustomNothing.packToBuilder(self, mutate b: builder) {
    b.storeUint(123, 32);
    b.storeRef(createEmptyCell());
}


fun Tensor3Skipping1.unpackFromSlice(mutate s: slice): Tensor3Skipping1 {
    val e0 = s.loadUint(8);
    val e2 = s.loadUint(8);
    return (e0, 0, e2);
}

fun Tensor3Skipping1.packToBuilder(self, mutate b: builder) {
    b.storeUint(self.0, 8).storeUint(self.2, 8);
}

fun Color.unpackFromSlice(mutate s: slice) {
    return s.loadUint(5) as Color
}

fun Color.packToBuilder(self, mutate b: builder) {
    b.storeUint(self as int, 5)
}

type OnlyWithPack = int8

fun OnlyWithPack.packToBuilder(self, mutate b: builder) {
    b.storeUint(0xFF, 8);
    b.storeUint(self, 8);
}

type OnlyWithUnpack = int

fun OnlyWithUnpack.unpackFromSlice(mutate s: slice): int {
    assert (s.loadUint(8) == 0xFF) throw 123;
    return s.loadUint(8)
}

struct HasOnlyWithUnpack {
    wu: OnlyWithUnpack
}

fun onInternalMessage() {}
