contract LotsOfMessages {
    incomingMessages: AllowedMessages
    storage: BasicStorage<int64>
}

type AllowedMessages = IncreaseBy | ResetTo<int64> | EmptyMsg
type StorageInst = BasicStorage<int64>

const ONE = 0 + 1

const ERR_INVALID_COUNTER_ID = 100;

struct BasicStorage<T> {
    counter_id: int32 = 0
    counter_value: T
}

struct (0x12345678) IncreaseBy {
    counter_id: int32
    inc_by: int32 = ONE
}

struct (0x23456789) ResetTo<T> {
    counter_id: int32
    reset_to: T = 0
}

struct (0x34567890) EmptyMsg {
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessages.fromSlice(in.body);
    var st = StorageInst.fromCell(contract.getData());

    match (msg) {
        IncreaseBy => {
            assert (msg.counter_id == st.counter_id) throw ERR_INVALID_COUNTER_ID;
            st.counter_value += msg.inc_by;
        }
        ResetTo<int64> => {
            assert (msg.counter_id == st.counter_id) throw ERR_INVALID_COUNTER_ID;
            st.counter_value = msg.reset_to;
        }
        EmptyMsg => {}
    }

    contract.setData(st.toCell());
}

get fun currentCounter(): int {
    val st = lazy StorageInst.fromCell(contract.getData());
    return st.counter_value;
}
