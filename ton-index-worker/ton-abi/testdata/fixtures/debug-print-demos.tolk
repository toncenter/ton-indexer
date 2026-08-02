// this contract has lots of get methods returning something
// from TypeScript, we check `debugPrintFromStack` for each of returns

import "@stdlib/reflection"
import "@stdlib/strings"

contract DebugPrintDemos {
    incomingMessages: ()
    storage: DebugStorage
}

struct DebugStorage {
    dummy: uint32
}

struct WithWeirdUnions {
    u1: Point | ((), Point)
    u2: () | int
    u3: () | (int, int) | null
}

fun onInternalMessage(in: InMessage) {
}

struct Point {
    x: int = 10
    y: int = 20
}

struct Named {
    name: string
    value: int32
}

struct Wrapper<T> {
    item: T
}

struct Nested {
    p: Point
    w: Wrapper<int32>
}

const EMPTY_STR = ""

struct Empty

enum Color {
    Red,
    Green,
    Blue,
}

enum Sign: int8 {
    Negative = -1,
    Zero = 0,
    Positive = 1,
}

struct WithCells {
    r1: Cell<int32>?
    r2: Cell<int32>?
    r3: Cell<Cell<bool>>
    r4: array<Cell<any_address>>
}

type IntOrBool = int | bool
type PointOrNull = Point | null
type ArrayOfInt = array<int>



get fun primitives(): (int32, uint8, bool, coins) {
    return (42, 255, true, ton("1"))
}

get fun justStrings(): (string, string, string) {
    return ("hello", EMPTY_STR, reflect.typeNameOf<int32>())
}

get fun structures1(): (Point, Empty) {
    return (
        { x: -10, y: 20 },
        {},
    );
}

get fun structures2(): Nested {
    return { p: { x: 5, y: 6 }, w: { item: 99 } };
}

get fun enumValues(): (Color, Color, Sign) {
    return (Color.Red, Color.Blue, Sign.Negative);
}

get fun nullables1(): (int?, int?, bool?, bool?) {
    return (null, 42, null, true);
}

get fun arrays1(): (array<int>, array<Point>, ArrayOfInt) {
    return ([1, 2, 3], [{ x: 10, y: 20 }, { x: 30, y: 40 }], []);
}

get fun shapedTuple1(): [int32, Wrapper<bool>, Point] {
    return [10, { item: false }, { x: 1, y: 2 }];
}

get fun union1(): IntOrBool {
    return 42;
}

get fun union2(): (IntOrBool | (int8, int16), IntOrBool | (int8, int16)) {
    return (true, (8, 16));
}

get fun unionNull(): (PointOrNull, PointOrNull) {
    return ({ x: 7, y: 8 }, null);
}

get fun unionNullInShape(): [PointOrNull, PointOrNull] {
    return [{ x: 7, y: 8 }, null];
}

get fun cellRefs(): (Cell<int8>, Cell<Wrapper<int8 | int16>>) {
    return ((8 as int8).toCell(), Wrapper{item: (10 as int16) as int8|int16}.toCell());
}

get fun cellOfCells(): WithCells {
    return {
        r1: (5 as int32).toCell(),
        r2: null,
        r3: true.toCell().toCell(),
        r4: [createAddressNone().toCell(), (address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c") as any_address).toCell()],
    }
}

get fun maps(): (map<int8, int32?>, map<int8, int32>) {
    var m: map<int8, int32?> = createEmptyMap();
    m.set(1, 100);
    m.set(2, null);
    return (m, []);
}

get fun nullableMaps(): (map<int8, int32>?, map<int8, int32>?) {
    return ([], null);
}

get fun lispList1(): lisp_list<int> {
    return [10, 20, 30];
}

get fun address1(): address {
    return address("0:527964d55cfa6eb731f4bfc07e9d025098097ef8505519e853986279bd8400d8");
}

get fun addressOpt(): (address?, address) {
    return (null, address1());
}

get fun wrapperPoint(): Wrapper<Point> {
    return { item: { x: -1, y: 1 } };
}

get fun voidReturn() {
}

get fun wideNullables(): (Point?, Point?, (int, int)?, (int, int)?) {
    return ({ x: 3, y: 4 }, null, (3, 4), null);
}

get fun combo1(): (int32, Point, Color, array<string>) {
    return (777, { x: -100, y: 200 }, Color.Green, ["test"]);
}

get fun weirdUnion1(): WithWeirdUnions {
    return {u1: {}, u2: (), u3: ()}
}

get fun weirdUnion2(): WithWeirdUnions? {
    return {u1: ((), {y: 70}), u2: 3, u3: null}
}

get fun rawCell(): cell {
    return createEmptyCell();
}

get fun rawSlices(): (builder, bits8) {
    return (beginCell().storeRef(beginCell().storeSlice("abcd".literalSlice()).endCell()), "ab".literalSlice() as bits8);
}

get fun unknowns(): tuple {
    return [1, "aba", "ab".literalSlice(), Point { x: 10, y: 20 }, null];
}
