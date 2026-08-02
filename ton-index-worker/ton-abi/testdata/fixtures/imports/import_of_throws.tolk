struct D {}

const D_ERR = 999 - 1

fun D.more() {
    assert (true, D_ERR);       // not exported to abi, because it's in unused call graph
}
