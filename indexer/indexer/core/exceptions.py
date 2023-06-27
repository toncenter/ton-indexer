class DataNotFound(Exception):
    pass

class BlockNotFound(DataNotFound):
    def __init__(self, workchain, shard, seqno):
        self.workchain = workchain
        self.shard = shard
        self.seqno = seqno

    def __str__(self):
        return f"Block ({self.workchain}, {self.shard}, {self.seqno}) not found in DB"

class TransactionNotFound_Deprecated(DataNotFound):
    def __init__(self, lt, hash):
        self.lt = lt
        self.hash = hash

    def __str__(self):
        return f"Transaction ({self.lt}, {self.hash}) not found in DB"

class TransactionNotFound(DataNotFound):
    def __init__(self, hash):
        self.hash = hash

    def __str__(self):
        return f"Transaction {self.hash} not found in DB"


class MessageNotFound(DataNotFound):
    def __init__(self, source, destination, created_lt):
        self.source = source
        self.destination = destination
        self.created_lt = created_lt

    def __str__(self):
        return f"Message not found source: {self.source} destination: {self.destination} created_lt: {self.created_lt}"
