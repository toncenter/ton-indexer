class TimeoutError(Exception):
    def __str__(self):
        return f"Query timeout"


class DataNotFound(Exception):
    pass


class MultipleDataFound(Exception):
    pass


class BlockNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"Block not found in DB: {kvs}"


class TransactionNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"Transaction not found in DB: {kvs}"
    

class MultipleTransactionsFound(MultipleDataFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"Multiple transactions found in DB: {kvs}"


class MessageNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"Message not found in DB: {kvs}"
    

class NFTCollectionNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"NFT collection not found in DB: {kvs}"


class NFTItemNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"NFT item not found in DB: {kvs}"


class NFTTransferNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"NFT transfer not found in DB: {kvs}"


class JettonMasterNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"Jetton master not found in DB: {kvs}"


class JettonWalletNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"Jetton wallet not found in DB: {kvs}"


class JettonTransferNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"Jetton transfer not found in DB: {kvs}"


class JettonBurnNotFound(DataNotFound):
    def __init__(self, 
                 **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        kvs = ', '.join(f'{k}: {v}' for k, v in self.kwargs.items())
        return f"Jetton burn not found in DB: {kvs}"
