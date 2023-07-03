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
