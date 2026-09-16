import "imports/import_of_throws"

contract LotsOfThrows {
    incomingMessages: ()
    storage: null
    forceAbiExport: WithUnsupportedDefaults
}

const ONE = 1
const ERR_100 = 100
const ERR_101 = 100 + ONE
const ERR_102 = ERR_101 + (1 * 1 / 1)
const ERR_103 = 104 + -1
const ERR_104 = 120 - (1 << 4)
/// desc for 105
const ERR_105 = 100 + ((ONE * ONE) | 4)
const AGAIN_105 = - -(((ERR_105)))

const CantDeserializePoint = 9999
const CantGetMapElement = 10000

enum Err {
    EInEnum1 = 80,
    /// desc for EInEnum2
    EInEnum2,
}

struct Point {
    x: int8
    y: int8
}

struct WithUnsupportedDefaults {
    f1: int | slice = 10
    f2: int8 | int32 = 10 as int32
    f3: Err | Point = Err.EInEnum2
    f4: (int, [bool | address]) = (5, [true])
    f5: array<int | slice> = [1, 2]
    f6: lisp_list<bool | builder> = []
}

fun main() {
    throw ERR_100;
    throw (ERR_101);
    throw (ERR_102, "");
    assert (true) throw ERR_103;
    assert (true, ERR_104);
    assert (true) throw ERR_105;
    assert (true) throw (((AGAIN_105)));

    Point.fromCell(createEmptyCell(), {
        throwIfOpcodeDoesNotMatch: CantDeserializePoint,
    });

    var m = createEmptyMap<int32, int64>();
    m.mustGet(100, CantGetMapElement);

    if (random.uint256() < 0) {
        throw Err.EInEnum1;
    }
    assert (random.uint256() != 0) throw Err.EInEnum2;

    // these are exported to IR, but not to TypeScript
    throw 1234;
    assert (true, 2345);
    throw ERR_100 * 2;
}

fun onExternalMessage(inMsg: slice) {

}

fun unusedF() {
    D.more();
}
