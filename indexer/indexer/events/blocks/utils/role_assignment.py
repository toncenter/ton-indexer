from __future__ import annotations

from typing import Callable

from indexer.core.database import Action, ROLE_SENDER, ROLE_RECEIVER, ROLE_SENDER_RECEIVER


def _default_role_assignment(action: Action) -> dict[str, int]:
    """Assign roles based on source/destination fields.

    source, source_secondary -> ROLE_SENDER
    destination, destination_secondary -> ROLE_RECEIVER
    accounts in both sets -> ROLE_SENDER_RECEIVER
    accounts in neither set -> ROLE_SENDER_RECEIVER (mediators/unknown)
    """
    sender_accounts = set()
    receiver_accounts = set()

    if action.source is not None:
        sender_accounts.add(action.source)
    if action.source_secondary is not None:
        sender_accounts.add(action.source_secondary)
    if action.destination is not None:
        receiver_accounts.add(action.destination)
    if action.destination_secondary is not None:
        receiver_accounts.add(action.destination_secondary)

    roles = {}
    for account in action.accounts:
        is_sender = account in sender_accounts
        is_receiver = account in receiver_accounts
        if is_sender and is_receiver:
            roles[account] = ROLE_SENDER_RECEIVER
        elif is_sender:
            roles[account] = ROLE_SENDER
        elif is_receiver:
            roles[account] = ROLE_RECEIVER
        else:
            roles[account] = ROLE_SENDER_RECEIVER
    return roles


def _swap_role_assignment(action: Action) -> dict[str, int]:
    """Custom role assignment for jetton_swap actions.

    In swaps, most accounts act as both sender and receiver (intermediary pools, wallets).
    Only the pure sender (initiator) gets ROLE_SENDER.
    """
    sender_accounts = set()
    if action.source is not None:
        sender_accounts.add(action.source)

    roles = {}
    for account in action.accounts:
        if account in sender_accounts:
            roles[account] = ROLE_SENDER
        else:
            roles[account] = ROLE_SENDER_RECEIVER
    return roles


_custom_role_functions: dict[str, Callable[[Action], dict[str, int]]] = {
    'jetton_swap': _swap_role_assignment,
}


def assign_roles(action: Action) -> dict[str, int]:
    """Entry point for role assignment. Dispatches to custom or default handler."""
    handler = _custom_role_functions.get(action.type, _default_role_assignment)
    return handler(action)
