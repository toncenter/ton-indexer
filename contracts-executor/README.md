## Contract executor HTTP API

Simple wrapper around great tool from ton-community: [ton-contract-executor](https://github.com/ton-community/ton-contract-executor).
Starts an HTTP server with ``POST /execute`` method:

```json
{
    "code": "te6cckECDQEA...", # base64 encoded code cell
    "data": "te6cckECDQEA...", # base64 encoded data cell
    "method": "get_data", # get-method data,
    "gas_limit": 10000 # gas limit
    "arguments": [], # get-method arguments
    "address": "EQ...", # contract address
    "expected": ["int", "int", "address", "metadata", "cell_hash"], # method signature
    "parse_list": true # parse output as a lisp-style list
}
```
            
``expected`` field should contain a list of expected value types for the method
being executed. It is required to format returned value from internal representation to
some meaningful form. Supported types:
* ``int`` - converts BN to number (returned as string, not int)
* ``address`` - reads address from slice and converts to friendly format (``EQ..``)
* ``cell_hash`` - computes Cell hash
* ``boc`` - returns base64 representation of cell
* ``string`` - returns string representation of slice
* ``metadata`` - reads [TEP-64](https://github.com/ton-blockchain/TEPs/blob/master/text/0064-token-data-standard.md) metadata

Returns list of results 
```
{
    "type": "success",
    "exit_code": 0,
    "gas_consumed": 685,
    "result": [
        12312321,
        -1,
        "EQ_________________________________1",
        {
            "content_layout": "on-chain",
            "content": {
                "name": "Name",
                "symbol": "Symbol"
            }
        },
        "zScFSDDDSN1XMaS2UzYDDDejOtgpJSFpCK0sh8VDKUo="
    ],
    "action_list_cell": {
        "refs": [],
        "kind": "ordinary",
        "bits": {}
    },
    "logs": "",
    "actionList": [],
    "debugLogs": []
}
```