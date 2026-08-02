import "@stdlib/lisp-lists"
import "@stdlib/reflection"
import "@stdlib/tvm-dicts"
import "@stdlib/strings"
import "imports/import_of_tricky"

contract LotsOfGetters {
    incomingMessages: IncreaseCounter | ResetCounterAlias | TransferNotification<uint32>
    storage: `Storage@me`
}

// this struct defines storage layout of the contract
struct `Storage@me` {
    id: uint32  // required to allow multiple independent counter instances, since the contract address depends on its initial state
    counter: uint32 // the current counter value
}

fun `Storage@me`.load() {
    return `Storage@me`.fromCell(contract.getData())
}

fun `Storage@me`.save(self) {
    contract.setData(self.toCell())
}

struct (0x7e8764ef) IncreaseCounter {
    queryId: uint64
    increaseBy: uint32
}

struct (0xF0F0F0F0) TransferNotification<PayloadT> {
    payload: PayloadT
}

struct Point {
    x: int = 10
    y: int = 20
}

struct WithWeirdUnions {
    u1: Point | ((), Point)
    u2: () | int
    u3: () | (int, int) | null
}

// using unions to represent available messages
// this allows processing them with pattern matching
type AllowedMessage = IncreaseCounter | ResetCounterAlias | TransferNotification<uint32>

// the main entrypoint: called when a contract receives an message from other contracts
fun onInternalMessage(in: InMessage) {
    // use `lazy` to defer loading fields until they are accessed
    val msg = lazy AllowedMessage.fromSlice(in.body);

    match (msg) {
        IncreaseCounter => {
            // load contract storage lazily (efficient for large or partial reads/updates)
            var storage = lazy `Storage@me`.load();

            storage.counter += msg.increaseBy;
            storage.save();
        }

        ResetCounter => {
            var storage = lazy `Storage@me`.load();

            storage.counter = 0;
            storage.save();
        }

        TransferNotification => {
            var storage = lazy `Storage@me`.load();

            storage.counter = msg.payload;
            storage.save();
        }

        else => {
            // ignore empty messages, "wrong opcode" for others
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}

// a handler for bounced messages (not used here, may be ommited)
fun onBouncedMessage(in: InMessageBounced) {
}

// get methods are a means to conveniently read contract data using, for example, HTTP APIs
// note that unlike in many other smart contract VMs, get methods cannot be called by other contracts
get fun currentCounter(): int {
    val storage = lazy `Storage@me`.load();
    return storage.counter;
}

get fun initialId(): int {
    val storage = lazy `Storage@me`.load();
    return storage.id;
}

fun bits16.bitsEqual(self, s: slice) {
    return (self as slice).bitsEqual(s)
}

// 128 bytes = 1024 bits, needs snake (one ref to 1 byte)
const STR_128 = "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678";

struct Wrapper<T> {
    item: T;
}

struct WrapperN<T> {
    item: T?
}

struct WithWrapper<T> {
    nestedW: Wrapper<T>
}

type WithWrapperAlias1<K> = WithWrapper<K>
type WithWrapperAlias2<T> = WithWrapperAlias1<T>

struct NestedPartReply {
    n1: bool
    n2: slice
}

struct PartReply {
    f1: int
    f2: tuple
    f3: (builder, NestedPartReply)
}

struct Empty

enum Color {
    Red,
    Green,
    Blue,
}

enum E0Max {
    ZERO = 0,
    MAX_INT = 115792089237316195423570985008687907853269984665640564039457584007913129639935,
}

type MyInt32 = int32

struct WithCells {
    c1: Cell<MyInt32>
    c2: Cell<Wrapper<int32>>?
    c3: Cell<WithWrapperAlias2<int64>>?
    c4: Cell<int32 | int64>
    c5: Cell<dict>
    c6: Cell<Color>
}

get fun complexResponse1(): (any_address, PartReply, tuple, (RemainingBitsAndRefs, int, int100), dict, string, string) {
    var f2 = createEmptyTuple();
    f2.push(null);
    return (
        createAddressNone(),
        {
            f1: 0,
            f2: f2,
            f3: (beginCell().storeUint(123, 32), { n1: true, n2: stringHexToSlice("0102") }),
        },
        createEmptyTuple(),
        (createEmptySlice(), 123, 1<<99),
        createEmptyDict(),
        "hello",
        STR_128,
    )
}

get fun complexResponse2(): (varint32, coins, [dict], cell, Cell<int8>, Empty, [Color, E0Max], [], PackOptions) {
    var dict = createEmptyDict();
    dict.uDictSetBuilder(32, 1, beginCell().storeInt(10, 32));
    return (
        500,
        ton("0.05"),
        [dict],
        (8 as int8).toCell(),
        (8 as int8).toCell(),
        {},
        [Color.Blue, E0Max.MAX_INT],
        [],
        { skipBitsNValidation: true },
    )
}

get fun complexResponse3(returnNull: bool): (int?, coins?, cell?, slice?, builder?, bool?, address?, tuple?, bits16?, Cell<int8>?, Cell<Cell<int8>>?, Wrapper<int?>, WithWrapperAlias2<Wrapper<bool?>>, [int, [int8]]?, string?, null) {
    return (
        returnNull ? null : 8,
        returnNull ? null : ton("0.05"),
        returnNull ? null : createEmptyCell(),
        returnNull ? null : stringHexToSlice("0102"),
        returnNull ? null : beginCell().storeUint(0xFF, 32),
        returnNull ? null : true,
        returnNull ? null : address("9:527964d55cfa6eb731f4bfc07e9d025098097ef8505519e853986279bd8400d8"),
        returnNull ? null : [1, 2] as tuple,
        returnNull ? null : stringHexToSlice("0102") as bits16,
        returnNull ? null : (8 as int8).toCell(),
        returnNull ? null : (8 as int8).toCell().toCell(),
        { item: returnNull ? null : 123 },
        { nestedW: { item: { item: returnNull ? null : true } } },
        returnNull ? null : [1, [2]],
        returnNull ? null : "spoon",
        null,
    )
}

get fun complexResponse4(): (WithCells, Cell<PackOptions>, map<int8, coins>, map<int8, bool>, map<address, Color>?, map<address, Color>?) {
    var m1: map<int8, coins> = createEmptyMap();
    m1.set(8, ton("0.05"));
    var m3: map<address, Color> = createEmptyMap();
    m3.set(address("9:527964d55cfa6eb731f4bfc07e9d025098097ef8505519e853986279bd8400d8"), Color.Blue);

    return (
        {
            c1: (1 as MyInt32).toCell(),
            c2: null,
            c3: WithWrapper { nestedW: { item: 3 as int64 } }.toCell(),
            c4: ((4 as int64) as int32 | int64).toCell(),
            c5: createEmptyDict().toCell(),
            c6: Color.Green.toCell(),
        },
        PackOptions { skipBitsNValidation: true }.toCell(),
        m1,
        createEmptyMap(),
        m3,
        null,
    )
}

get fun complexResponse5(): (WrapperN<coins>, WrapperN<coins>, WrapperN<slice>, WrapperN<slice>, WrapperN<address>, WrapperN<any_address>, WrapperN<any_address>, WrapperN<any_address>) {
    return (
        { item: null },
        { item: 123 },
        { item: null },
        { item: stringHexToSlice("01") },
        { item: null },
        { item: null },
        { item: createAddressNone() },
        { item: address("9:527964d55cfa6eb731f4bfc07e9d025098097ef8505519e853986279bd8400d8") as any_address },
    )
}

get fun arraysResponse1(): (tuple, array<int>, array<int?>, array<Point>, array<()>, array<(int, int)>, array<string>) {
    return (
        [1, true, address("9:527964d55cfa6eb731f4bfc07e9d025098097ef8505519e853986279bd8400d8"), createAddressNone(), Point{}],
        [1, 2, 3],
        [1, null, 3],
        [{}, {x: 100, y: 200}],
        [(), ()],
        [(10, 20), (30, 40)],
        ["one", "two"],
    )
}

get fun arraysResponse2(): (tuple, array<int>, array<int?>, array<Point>, array<()>, array<(int, int)>, array<string>) {
    return (
        [],
        [],
        [],
        [],
        [],
        [],
        [],
    )
}

get fun arraysResponse3(): (array<int8>, array<array<int>>, Cell<array<int8>>, array<Cell<int8>>, array<int>?, array<int?>?, array<Point>?, Cell<array<(int16, int32)?>>) {
    return (
        [1, 2, 3],
        [[1,2],[],[5,6]],
        ([1, 2, 3] as array<int8>).toCell(),
        [(1 as int8).toCell(), (2 as int8).toCell(), (3 as int8).toCell()],
        [1, 2, 3],
        [1, null, 3],
        [Point{}, Point{x:-100, y:200}],
        ([(1, 2), null, null, (7, 8)] as array<(int16, int32)?>).toCell(),
    )
}

get fun arraysResponse4(): (array<int8>, array<array<int>>, Cell<array<int8>>, array<Cell<int8>>, array<int>?, array<int?>?, array<Point>?, Cell<array<(int16, int32)?>>, array<null>) {
    return (
        [],
        [],
        ([] as array<int8>).toCell(),
        [],
        [],
        null,
        null,
        ([] as array<(int16, int32)?>).toCell(),
        [null],
    )
}

get fun shapeResponse1(): ([int, Point, (), [Point], Wrapper<Point>, tuple, string], [(int8, int32?), Cell<(int8, int32?)>, cell?, array<int16>, array<int32>]) {
    return (
        [10, {}, (), [{}], { item: {} }, [1, createAddressNone()], STR_128],
        [(1, null), (8 as int8, null as int32?).toCell(), null, [], [1,2,3]],
    )
}

get fun lispResponse1(): (lisp_list<int>, lisp_list<int>, lisp_list<Point>, lisp_list<int8?>, lisp_list<()>, lisp_list<Wrapper<array<int32>>>, Cell<lisp_list<(int32, int64)>>, lisp_list<string>) {
    return (
        [1, 2, 3],
        [],
        [{x: -60, y: 60}, {}],
        [1, null, 3],
        [()],
        [{item: [1,2]}, {item: []}, {item: [5,6]}],
        ([(32, 64), (320, 640)] as lisp_list<(int32, int64)>).toCell(),
        ["one", "two"],
    )
}

struct WithWideNullables {
    pairN: (int, int)?
    pointN: Point?
}

struct OnlyIntN {
    i: int?
}

get fun wideNullableResponse1(): (OnlyIntN, OnlyIntN, OnlyIntN?, OnlyIntN?, OnlyIntN?, Wrapper<OnlyIntN>?, Wrapper<OnlyIntN>?, ((int), ())?, ((int), ())?) {
    return (
        { i: 0 },
        { i: null },
        { i: 2 },
        { i: null },
        null,
        { item: { i: null } },
        null,
        (7, ()),
        null,
    )
}

get fun wideNullableResponse2(): ((int, int)?, Point?, WithWideNullables, WithWideNullables, WithWideNullables, WithWideNullables?, WithWideNullables?) {
    return (
        (1, 2),
        {},
        { pairN: (5, 6), pointN: { x: 7, y: 8 }},
        { pairN: null, pointN: null },
        { pairN: (5, 6), pointN: null },
        { pairN: null, pointN: {} },
        null,
    )
}

type TwoInts = (int, int)
type CrazyUnionN = Cell<int8> | (int, int, int) | null

get fun unionResponse1(): (int | bool, int | bool, int | (int, int), int | TwoInts, int | Point, Point | int) {
    return (
        1,
        true,
        3,
        (4, 4),
        5,
        { x: 6, y: 6 },
    )
}

get fun unionResponse2(): (int | Point | (TwoInts, Point), CrazyUnionN, CrazyUnionN, CrazyUnionN, IncreaseCounter | TransferNotification<void>, Wrapper<int8 | int32 | IncreaseCounter>?, Wrapper<int8 | int32 | IncreaseCounter>?, Wrapper<int8 | int32 | IncreaseCounter>?) {
    return (
        { x: 0, y: 0 },
        (2 as int8).toCell(),
        (3, 3, 3),
        null,
        TransferNotification<void> {},
        { item: 6 as int32 },
        { item: { queryId: 7, increaseBy: 7 } },
        null,
    )
}

get fun unionResponse3(): (WithWeirdUnions, WithWeirdUnions, WithWeirdUnions?, WithWeirdUnions?) {
    return (
        {u1: {}, u2: (), u3: ()},
        {u1: ((), {}), u2: 2, u3: (2, 2)},
        {u1: ((), {y: 70}), u2: 3, u3: null},
        null,
    )
}

get fun complexParams1(n1: int, n2: coins, n3: (int, coins), n4: any_address, n5: slice, n6: cell, n7: builder, n8: tuple, n9: string, n10: string) {
    assert (n1 == 1) throw 123;
    assert (n2 == 2) throw 123;
    assert (n3.0 == 3) throw 123;
    assert (n3.1 == 4) throw 123;
    assert (n4.isNone()) throw 123;
    assert (n5.remainingBitsCount() == 16) throw 123;
    assert (n6.beginParse().remainingBitsCount() == 16) throw 123;
    assert (n7.bitsCount() == 16) throw 123;
    assert (n8.size() == 1) throw 123;
    assert (n9.equalTo("hello")) throw 123;
    assert (n10.equalTo(STR_128)) throw 123;
    return Wrapper<int> { item: n1 };
}

get fun complexParams2(w: WithWrapperAlias2<(int, coins)>, part: PartReply, rem: (RemainingBitsAndRefs, int123), a: [any_address], c: Cell<int8>, empty: Empty, et: [], e: [Color, E0Max], packO: PackOptions) {
    assert (w.nestedW.item.0 == 1) throw 123;
    assert (w.nestedW.item.1 == 2) throw 123;
    assert (part.f2.size() == 2) throw 123;
    assert (part.f3.1.n1 == true) throw 123;
    assert (rem.0.isEmpty()) throw 123;
    assert (rem.1 == 123) throw 123;
    assert (a.0.isInternal()) throw 123;
    assert (c.load() == 123) throw 123;
    assert (e.0 == Color.Blue) throw 123;
    assert (e.1 == E0Max.MAX_INT) throw 123;
    assert (packO.skipBitsNValidation == true) throw 123;
    return a;
}

get fun complexParams3(tens: (int?, coins?, cell?, slice?, builder?, bool?, address?, tuple?, bits16?, Cell<int8>?, Cell<Cell<int8>>?, Wrapper<int?>, Wrapper<Wrapper<bool?>>, [int, [int8]]?, string?, null), expectNull: bool): (int?, coins?) {
    if (expectNull) {
        assert (tens.0 == null) throw 123;
        assert (tens.1 == null) throw 123;
        assert (tens.2 == null) throw 123;
        assert (tens.3 == null) throw 123;
        assert (tens.4 == null) throw 123;
        assert (tens.5 == null) throw 123;
        assert (tens.6 == null) throw 123;
        assert (tens.7 == null) throw 123;
        assert (tens.8 == null) throw 123;
        assert (tens.9 == null) throw 123;
        assert (tens.10 == null) throw 123;
        assert (tens.11.item == null) throw 123;
        assert (tens.12.item.item == null) throw 123;
        assert (tens.13 == null) throw 123;
        assert (tens.14 == null) throw 123;
        assert (tens.15 == null) throw 123;
    } else {
        assert (tens.0! == 8) throw 123;
        assert (tens.1! == ton("0.05")) throw 123;
        assert (tens.2!.beginParse().bitsEqual(createEmptySlice())) throw 123;
        assert (tens.3!.bitsEqual("0102".hexToSlice())) throw 123;
        assert (tens.4!.bitsCount() == 32) throw 123;
        assert (tens.5! == true) throw 123;
        assert (tens.6 is address) throw 123;
        assert (tens.7!.size() == 2) throw 123;
        assert (tens.8!.bitsEqual("0102".hexToSlice())) throw 123;
        assert (tens.9!.load() == 8) throw 123;
        assert (tens.10!.load().load() == 8) throw 123;
        assert (tens.11.item! == 123) throw 123;
        assert (tens.12.item.item! == true) throw 123;
        assert (tens.13!.0 == 1) throw 123;
        assert (tens.13!.1.0 == 2) throw 123;
        assert (tens.14!.equalTo("spoon")) throw 123;
        assert (tens.15 == null) throw 123;
    }
    return (tens.0, tens.1);
}

get fun complexParams4(c: WithCells, cellO: Cell<PackOptions>, m1: map<int8, coins>, m2: map<int8, bool>, m3: map<address, Color>?, m4: map<address, Color>?) {
    val c4 = c.c4.load();

    assert (c.c1.load() == 1) throw 123;
    assert (c.c2 == null) throw 123;
    assert (c.c3!.load().nestedW.item == 3) throw 123;
    assert (c4 is int64 && c4 == 4) throw 123;
    assert (c.c5.load().dictIsEmpty()) throw 123;
    assert (c.c6.load() == Color.Green) throw 123;
    assert (cellO.load().skipBitsNValidation == true) throw 123;
    assert (m1.findFirst().getKey() == m1.findLast().getKey()) throw 123;
    assert (m1.mustGet(8) == ton("0.05")) throw 123;
    assert (m2.isEmpty()) throw 123;
    assert (m2.get(100).isFound == false) throw 123;
    assert (m3!.isEmpty() == false) throw 123;
    assert (m3!.findFirst().loadValue() == Color.Blue) throw 123;
    assert (m4 == null) throw 123;
}

get fun complexParams5(p1: WrapperN<coins>, p2: WrapperN<coins>, p3: WrapperN<slice>, p4: WrapperN<slice>, a1: WrapperN<address>, a2: WrapperN<any_address>, a3: WrapperN<any_address>, a4: WrapperN<any_address>) {
    assert (p1.item == null) throw 123;
    assert (p2.item != null && p2.item == 123) throw 123;
    assert (p3.item == null) throw 123;
    assert (p4.item != null && p4.item.remainingBitsCount() == 8) throw 123;
    assert (a1.item == null) throw 123;
    assert (a2.item == null) throw 123;
    assert (a3.item!.isNone()) throw 123;
    assert (a4.item!.getWorkchain() == 9) throw 123;
}

get fun arrayParams1(p1: tuple, p2: array<int>, p3: array<int?>, p4: array<Point>, p5: array<()>, p6: array<(int, int)>, p7: array<string>) {
    assert (p1.size() == 5) throw 123;
    assert (p1.get(1) as bool == true) throw 123;
    assert (p1.get(2) as address == address("9:527964d55cfa6eb731f4bfc07e9d025098097ef8505519e853986279bd8400d8")) throw 123;
    assert (p1.get(3) as any_address == createAddressNone()) throw 123;
    assert ((p1.get(4) as Point).y == 20) throw 123;
    assert (p2.size() == 3) throw 123;
    assert (p2.last() == 3) throw 123;
    assert (p3.get(1) == null) throw 123;
    assert (p3.get(2)! == 3) throw 123;
    assert (p4.first().x == 10) throw 123;
    assert (p4.last().y == 200) throw 123;
    assert (p5.size() == 2) throw 123;
    assert (p6.first().0 == 10) throw 123;
    assert (p6.last().1 == 40) throw 123;
    assert (p7.first().equalTo("one")) throw 123;
    assert (p7.last().equalTo("two")) throw 123;
}

get fun arrayParams2(p1: tuple, p2: array<int>, p3: array<int?>, p4: array<Point>, p5: array<()>, p6: array<(int, int)>, p7: array<string>) {
    assert (p1.size() == 0) throw 123;
    assert (p2.size() == 0) throw 123;
    assert (p3.size() == 0) throw 123;
    assert (p4.size() == 0) throw 123;
    assert (p5.size() == 0) throw 123;
    assert (p6.size() == 0) throw 123;
    assert (p7.size() == 0) throw 123;
}

get fun arrayParams3(p1: array<int8>, p2: array<array<int>>, p3: Cell<array<int8>>, p4: array<Cell<int8>>, p5: array<int>?, p6: array<int?>?, p7: array<Point>?, p8: Cell<array<(int16, int32)?>>) {
    val p8l = p8.load();
    assert (p1.size() == 3 && p1.last() == 3) throw 123;
    assert (p2.size() == 3 && p2.first().size() == 2 && p2.first().get(1) == 2 && p2.get(1).size() == 0 && p2.last().last() == 6) throw 123;
    assert (p3.load().size() == 3) throw 123;
    assert (p4.size() == 3 && p4.first().load() == 1 && p4.last().load() == 3) throw 123;
    assert (p5 != null && p5.size() == 3) throw 123;
    assert (p6 != null && p6.first()! == 1 && p6.get(1) == null) throw 123;
    assert (p7 != null && p7.size() == 2 && p7.get(0).x == 10 && p7.get(1).y == 100) throw 123;
    assert (p8l.size() == 4 && p8l.first() != null && p8l.first()!.0 == 1 && p8l.get(1) == null && p8l.last()!.1 == 8) throw 123;
}

get fun arrayParams4(p1: array<int8>, p2: array<array<int>>, p3: Cell<array<int8>>, p4: array<Cell<int8>>, p5: array<int>?, p6: array<int?>?, p7: array<Point>?, p8: Cell<array<(int16, int32)?>>) {
    assert (p1.size() == 0) throw 123;
    assert (p2.size() == 0) throw 123;
    assert (p3.load().size() == 0) throw 123;
    assert (p4.size() == 0) throw 123;
    assert (p5!.size() == 0) throw 123;
    assert (p6 == null) throw 123;
    assert (p7 == null) throw 123;
    assert (p8.load().size() == 0) throw 123;
}

get fun shapeParams1(p1: [int, Point, (), [Point], Wrapper<Point>, tuple, string], p2: [(int8, int32?), Cell<(int8, int32?)>, cell?, array<int16>, array<int32>]) {
    assert (p1.0 == 10) throw 123;
    assert (p1.1.x == 10 && p1.1.y == 20) throw 123;
    assert (p1.3.0.y == 20) throw 123;
    assert (p1.4.item.y == 20) throw 123;
    assert (p1.5.size() == 2) throw 123;
    assert (p1.6.equalTo(STR_128)) throw 123;
    assert ((p1.5.get(1) as any_address).isNone()) throw 123;
    assert (p2.0.0 == 1 && p2.0.1 == null) throw 123;
    assert (p2.1.load().0 == 8 && p2.1.load().1 == null) throw 123;
    assert (p2.2 == null) throw 123;
    assert (p2.3.size() == 0) throw 123;
    assert (p2.4.last() == 3) throw 123;
}

get fun lispParams1(p1: lisp_list<int>, p2: lisp_list<int>, p3: lisp_list<Point>, p4: lisp_list<int8?>, p5: lisp_list<()>, p6: lisp_list<Wrapper<array<int32>>>, p7: Cell<lisp_list<(int32, int64)>>, p8: lisp_list<string>) {
    assert (p1.popHead() == 1 && p1.popHead() == 2 && p1.popHead() == 3 && p1.isEmpty()) throw 123;
    assert (p2.isEmpty()) throw 123;
    assert (p3.popHead().x == -60 && p3.popHead().y == 20 && p3.isEmpty()) throw 123;
    assert (p4.popHead()! == 1 && p4.popHead() == null && p4.getHead()! == 3 && !p4.isEmpty()) throw 123;
    assert (!p5.isEmpty()) throw 123;
    assert (p6.getHead().item.size() == 2 && p6.popHead().item.first() == 1 && p6.popHead().item.size() == 0 && p6.popHead().item.last() == 6) throw 123;
    val p7l = p7.load();
    assert (p7l.getHead().0 == 32 && p7l.getHead().1 == 64 && p7l.getTail().getHead().1 == 640) throw 123;
    assert (p8.popHead().equalTo("one") && p8.popHead().equalTo("two") && p8.isEmpty()) throw 123;
}

get fun wideNullableParams1(p1: OnlyIntN, p2: OnlyIntN, p3: OnlyIntN?, p4: OnlyIntN?, p5: OnlyIntN?, p6: Wrapper<OnlyIntN>?, p7: Wrapper<OnlyIntN>?, p8: ((int), ())?, p9: ((int), ())?, final: int) {
    assert (p1.i! == 0) throw 123;
    assert (p2.i == null) throw 123;
    assert (p3!.i! == 2) throw 123;
    assert (p4!.i == null) throw 123;
    assert (p5 == null) throw 123;
    assert (p6!.item.i == null) throw 123;
    assert (p7 == null) throw 123;
    assert (p8!.0 == 7) throw 123;
    assert (p9 == null) throw 123;
    return final;
}

get fun wideNullableParams2(p1: (int, int)?, p2: Point?, p3: WithWideNullables, p4: WithWideNullables, p5: WithWideNullables, p6: WithWideNullables?, p7: WithWideNullables?, final: int) {
    assert (p1!.1 == 2) throw 123;
    assert (p2!.y == Point{}.y) throw 123;
    assert (p3.pairN!.1 == 6 && p3.pointN!.y == 8) throw 123;
    assert (p4.pairN == null && p4.pointN == null) throw 123;
    assert (p5.pairN!.1 == 6 && p5.pointN == null) throw 123;
    assert (p6!.pairN == null && p6!.pointN!.x == 10) throw 123;
    assert (p7 == null) throw 123;
    return final;
}

get fun unionParams1(p1: int | bool, p2: int | bool, p3: int | (int, int), p4: int | TwoInts, p5: int | Point, p6: Point | int, final: int) {
    assert (p1 is int && p1 == 1) throw 123;
    assert (p2 is bool && p2 == true) throw 123;
    assert (p3 is int && p3 == 3) throw 123;
    assert (p4 !is int && p4.1 == 4) throw 123;
    assert (p5 !is Point && p5 == 5) throw 123;
    assert (p6 is Point && p6.y == 6) throw 123;
    return final;
}

get fun unionParams2(p1: int | Point | (TwoInts, Point), p2: CrazyUnionN, p3: CrazyUnionN, p4: CrazyUnionN, p5: IncreaseCounter | TransferNotification<void>, p6: Wrapper<int8 | int32 | IncreaseCounter>?, p7: Wrapper<int8 | int32 | IncreaseCounter>?, p8: Wrapper<int8 | int32 | IncreaseCounter>?, final: int) {
    assert (p1 is Point && p1.y == 0) throw 123;
    assert (p2 is Cell && p2.load() == 2) throw 123;
    assert (p3 != null && p3 !is Cell && p3.2 == 3) throw 123;
    assert (p4 == null) throw 123;
    assert (p5 is TransferNotification) throw 123;
    assert (p6!.item is int32 && p6!.item == 6) throw 123;
    assert (p7!.item is IncreaseCounter && p7!.item.increaseBy == 7) throw 123;
    assert (p8 == null) throw 123;
    return final;
}

get fun unionParams3(p1: WithWeirdUnions, p2: WithWeirdUnions, p3: WithWeirdUnions?, p4: WithWeirdUnions?, final: int) {
    assert (p1.u1 is Point && p1.u1.y == 20) throw 123;
    assert (p1.u2 is ()) throw 123;
    assert (p1.u3 is ()) throw 123;
    assert (p2.u1 !is Point && p2.u1.1.y == 20) throw 123;
    assert (p2.u2 is int && p2.u2 == 2) throw 123;
    assert (p2.u3 is (int, int) && p2.u3.1 == 2) throw 123;
    assert (p3!.u1 !is Point && p3!.u1.1.y == 70) throw 123;
    assert (p3!.u2 is int && p3!.u2 == 3) throw 123;
    assert (p3!.u3 == null) throw 123;
    assert (p3!.u3 == null) throw 123;
    assert (p4 == null) throw 123;
    return final;
}

get fun defaultParams(explicit: int, d1: int = 50, d2: (int, address?) = (1, null), d3: lisp_list<int> = [1, 2], d4: Point = {}, d5: Point? = {}, d6: Point? = null, d7: CrazyUnionN = null) {
    assert (d1 == 50) throw 123;
    assert (d2.0 == 1) throw 123;
    assert (d2.1 == null) throw 123;
    assert (d3.popHead() == 1 && d3.popHead() == 2 && d3.isEmpty()) throw 123;
    assert (d4.y == 20) throw 123;
    assert (d5 != null && d5.x == 10) throw 123;
    assert (d6 == null) throw 123;
    assert (d7 == null) throw 123;
    return explicit;
}

get fun collisionParams(provider: int, StackReader: int, address: address, CellRef: Cell<any_address>, storeTolkAddress: bool, r: bool, c: Cell<slice>, string: string): (Cell<any_address>, int) {
    return (CellRef, StackReader)
}

get fun `processed?`(`asdf()`: int, `isBig?`: bool): int {
    return `isBig?` ? `asdf()` : 0;
}

get fun concatStrings(arr: array<string>): string {
    var sb = StringBuilder{};
    var idx = 0;
    repeat (arr.size()) {
        sb.append(arr.get(idx));
        idx += 1;
    }
    return sb.build();
}

get fun intAndPoint(i: int, p: Point) {
}
