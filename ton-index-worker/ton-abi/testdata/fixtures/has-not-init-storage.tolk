contract HasNotInitializedStorage {
    incomingMessages: ()
    storage: null
    storageAtDeployment: NftItemStorageNotInitialized
}


struct NftItemStorage {
    itemIndex: uint64
    collectionAddress: address
    ownerAddress: address
    content: cell
}

struct NftItemStorageNotInitialized {
    itemIndex: uint64
    collectionAddress: address
}

fun onInternalMessage() {}
