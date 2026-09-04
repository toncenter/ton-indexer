tolk 1.4

// Copy this template to abi/<contract>.tolk and add authoritative declarations.
// The contract directive is what makes tolk export them: messages go into
// incomingMessages, payload-only structs into forceAbiExport. The build picks
// the file up by glob.
struct (0x00000001) ExampleMessage {
    queryId: uint64;
    amount: coins;
    someAddress: address;
}

type AllowedMessage = ExampleMessage

contract TemplateContract {
    incomingMessages: AllowedMessage
}
