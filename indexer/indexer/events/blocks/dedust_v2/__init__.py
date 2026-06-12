from indexer.events.blocks.dedust_v2.claims import (
    DedustV2ClaimFeesBlockMatcher,
    DedustV2ClaimRewardBlockMatcher,
)
from indexer.events.blocks.dedust_v2.liquidity import (
    DedustV2DepositBlockMatcher,
    DedustV2DepositLiquidityBlock,
    DedustV2DepositLiquidityPartialBlock,
    DedustV2DepositPartialBlockMatcher,
    DedustV2RejectedDepositBlockMatcher,
    DedustV2WithdrawBlockMatcher,
    post_process_dedust_v2_liquidity,
)
from indexer.events.blocks.dedust_v2.swaps import (
    DedustV2FailedSwapBlockMatcher,
    DedustV2SwapBlockMatcher,
)


def matchers() -> list:
    return [
        DedustV2SwapBlockMatcher(),
        DedustV2FailedSwapBlockMatcher(),
        DedustV2WithdrawBlockMatcher(),
        DedustV2DepositBlockMatcher(),
        DedustV2RejectedDepositBlockMatcher(),
        DedustV2DepositPartialBlockMatcher(),
        DedustV2ClaimFeesBlockMatcher(),
        DedustV2ClaimRewardBlockMatcher(),
    ]
