from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(eq=False)
class Registries:
    # eq=False keeps identity-based __hash__/__eq__, which lets Registries serve as
    # a weakref key in the compiler's subclass cache.
    opcodes:       dict[str, int]                                    = field(default_factory=dict)
    predicates:    dict[str, Callable[[Any], bool]]                  = field(default_factory=dict)
    block_types:   dict[str, type]                                   = field(default_factory=dict)
    builders:      dict[str, Callable[..., Awaitable[Any]]]          = field(default_factory=dict)
    # message_types: message parser class by name; constructed from a pytoniq_core Slice.
    # lookups: async interface resolvers, e.g. await lookups["jetton_wallet"](addr).
    # Both empty by default; populate via default_message_types()/default_lookups().
    message_types: dict[str, type]                                   = field(default_factory=dict)
    lookups:       dict[str, Callable[..., Awaitable[Any]]]          = field(default_factory=dict)
    # fns: host-function escape hatch for build expressions — async callables
    # invoked by plain call syntax when the name is not a builtin. Unlike
    # builtins/lookups, null arguments are passed through (an absent optional
    # capture is a legitimate input to imperative host logic).
    fns:           dict[str, Callable[..., Awaitable[Any]]]          = field(default_factory=dict)
    # shapers: post-build tree surgery hooks (`shape IDENT`) — SYNC
    # callables `(produced_block, match) -> None` invoked by the engine after
    # the produced block's merge. May mutate only the produced block's region.
    shapers:       dict[str, Callable[..., Any]]                     = field(default_factory=dict)

    def register_predicate(self, name: str) -> Callable[[F], F]:
        def deco(fn: F) -> F:
            if name in self.predicates:
                raise KeyError(f"predicate {name!r} already registered")
            self.predicates[name] = fn  # type: ignore[assignment]
            return fn
        return deco

    def register_builder(self, name: str) -> Callable[[F], F]:
        def deco(fn: F) -> F:
            if name in self.builders:
                raise KeyError(f"builder {name!r} already registered")
            self.builders[name] = fn  # type: ignore[assignment]
            return fn
        return deco

    def register_message_type(self, name: str, cls: type) -> type:
        # Message classes live in indexer.events.blocks.messages and pre-exist, so
        # this is a plain two-arg register (not a decorator like the ones above).
        if name in self.message_types:
            raise KeyError(f"message type {name!r} already registered")
        self.message_types[name] = cls
        return cls

    def register_lookup(self, name: str) -> Callable[[F], F]:
        def deco(fn: F) -> F:
            if name in self.lookups:
                raise KeyError(f"lookup {name!r} already registered")
            self.lookups[name] = fn  # type: ignore[assignment]
            return fn
        return deco

    def register_fn(self, name: str) -> Callable[[F], F]:
        def deco(fn: F) -> F:
            if name in self.fns:
                raise KeyError(f"fn {name!r} already registered")
            self.fns[name] = fn  # type: ignore[assignment]
            return fn
        return deco

    def register_shaper(self, name: str) -> Callable[[F], F]:
        def deco(fn: F) -> F:
            if name in self.shapers:
                raise KeyError(f"shaper {name!r} already registered")
            self.shapers[name] = fn  # type: ignore[assignment]
            return fn
        return deco


def default_message_types() -> dict[str, type]:
    """Message parser classes keyed by class name (jetton family + swap messages
    referenced by the spec files in mch/specs/).

    Lazy import: pulling blocks.messages at module-import time would drag the full
    protocol/DB/context stack into the otherwise lightweight mch package.
    """
    from indexer.events.blocks.messages import jettons, swaps

    classes = (
        jettons.JettonTransfer,
        jettons.JettonInternalTransfer,
        jettons.JettonNotify,
        jettons.JettonBurn,
        jettons.JettonBurnNotification,
        jettons.JettonMint,
        jettons.MinterJettonMint,
        swaps.DedustSwap,
        swaps.DedustSwapExternal,
        swaps.DedustSwapPeer,
        swaps.DedustSwapNotification,
        swaps.DedustPayoutFromPool,
        swaps.DedustPayout,
        swaps.StonfiV2PayTo,
        swaps.PTonTransfer,
    )
    return {cls.__name__: cls for cls in classes}


def default_lookups() -> dict[str, Callable[..., Awaitable[Any]]]:
    """Async interface resolvers backed by the interface_repository contextvar.

    Addresses are normalized with .upper() to match interface-repository keys,
    which are raw-form uppercase (see event_processing / blocks.jettons callers).
    """
    from indexer.events import context

    async def jetton_wallet(address: str) -> Any:
        repo = context.interface_repository.get()
        if repo is None:
            return None
        return await repo.get_jetton_wallet(address.upper())

    async def nft_item(address: str) -> Any:
        repo = context.interface_repository.get()
        if repo is None:
            return None
        return await repo.get_nft_item(address.upper())

    return {
        "jetton_wallet": jetton_wallet,
        "nft_item": nft_item,
    }
