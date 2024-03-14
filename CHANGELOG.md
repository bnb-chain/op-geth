# Changelog

## v0.3.0

This is a recommended release for op-geth. This release brings in upstream updates, see https://github.com/bnb-chain/op-geth/pull/58 for the contents. This is also a ready release for the next planed fork, which will bring in canyon fork from upstream as well.

### User Facing Changes

- Feature of config tries layer number in memory is removed, and related flag `--triesInMemory` is deleted.
- If start a fresh node, will use pebble db as default(set `--db.engine=leveldb` if prefer leveldb)
- You can start opbnb mainnet or testnet without init genesis.json, and use flag `--opBNBMainnet` or `--opBNBTestnet` to start new node. If you have already inited genesis.json, you can still use these two flags, which will check whether the content of your init is correct.

### Partial Changelog

- [#64](https://github.com/bnb-chain/op-geth/pull/64): fix(op-geth):fix static peer cannot reassign header tasks
- [#60](https://github.com/bnb-chain/op-geth/pull/60): feature: add opbnb networks' genesis 
- [#58](https://github.com/bnb-chain/op-geth/pull/58): Merge upstream op-geth v1.101304.1
- [#51](https://github.com/bnb-chain/op-geth/pull/51): feat: sync pebble db from bsc
- [#50](https://github.com/bnb-chain/op-geth/pull/50): fix: fix task stuck and not reassign bug in concurrent-fetch logic
- [#49](https://github.com/bnb-chain/op-geth/pull/49): fix: ignore errors that caused by gap to keep peer connection 
- [#46](https://github.com/bnb-chain/op-geth/pull/46): fix: prune uses the latest block height as the target by default

### Docker Images

- ghcr.io/bnb-chain/op-geth:v0.3.0

### Full Changelog

https://github.com/bnb-chain/op-geth/compare/v0.2.2...v0.3.0

## v0.2.2

This is a minor release for opBNB Mainnet and Testnet.
It primarily optimizes op-geth and introduces an option to re-announce remote transactions.
Upgrading is optional.

### User Facing Changes

- The startup node will default to using the bootnodes of the opBNB mainnet. If the `--networkid=` is configured as testnet, the testnet bootnodes will be used. If `--bootnodes=` is configured, the specified bootnodes will be used. The configured `--bootnodes=` take precedence over other options.[#32](https://github.com/bnb-chain/op-geth/pull/32)
- Enable re-announce remote transactions by using the flag `--txpool.reannounceremotes=true`.[#33](https://github.com/bnb-chain/op-geth/pull/33)

### Partial Changelog

- [#14](https://github.com/bnb-chain/op-geth/pull/14): fix: add special logic to handle ancestor errors[BNB-3]
- [#16](https://github.com/bnb-chain/op-geth/pull/16): fix: wrong event log value
- [#17](https://github.com/bnb-chain/op-geth/pull/17): fix: cache data after successful writing[BNB-12]
- [#19](https://github.com/bnb-chain/op-geth/pull/19): fix: handle error in state_prefetcher.go and blockchain.go[BNB-16]
- [#20](https://github.com/bnb-chain/op-geth/pull/20): fix: refraining from using gopool for long-running tasks[BNB-19]
- [#21](https://github.com/bnb-chain/op-geth/pull/21): fix: remove redundant lock[BNB-20]
- [#22](https://github.com/bnb-chain/op-geth/pull/22): fix: remove unnecessary newRPCTransactionFromBlockHash function[BNB-21]
- [#31](https://github.com/bnb-chain/op-geth/pull/31): ci: fix blst error and unknown architecture
- [#32](https://github.com/bnb-chain/op-geth/pull/32): feature: add opBNB bootnodes
- [#33](https://github.com/bnb-chain/op-geth/pull/33): feat: add option to reannounce remote transactions
- [#34](https://github.com/bnb-chain/op-geth/pull/34): fix: clear underpriced buffer
- [#41](https://github.com/bnb-chain/op-geth/pull/41): txpool: enhance some logs and metrics on broadcasting and annoucing
- [#43](https://github.com/bnb-chain/op-geth/pull/43): chore: add reannounce metric for txpool
- [#44](https://github.com/bnb-chain/op-geth/pull/44): chore: impr/add some metrics txpool
- [#45](https://github.com/bnb-chain/op-geth/pull/45): feat: add TrieCommitInterval configuration, commit trie every TrieCommitInterval blocks

### Docker Images

- ghcr.io/bnb-chain/op-geth:v0.2.2

### Full Changelog

https://github.com/bnb-chain/op-geth/compare/v0.2.1...v0.2.2

## v0.2.1

This is the Fermat Hardfork release for opBNB Mainnet.
It will be activated at block height 9397477, expected to occur on November 28, 2023, at 6 AM UTC.

All mainnet nodes must upgrade to this release before the hardfork.
Upgrading for other networks is optional.

### User Facing Changes

NA

### Partial Changelog

- #30: feat: add opbnb mainnet Fermat fork height

### Docker Images

- ghcr.io/bnb-chain/op-geth:v0.2.1

### Full Changelog

https://github.com/bnb-chain/op-geth/compare/v0.2.0...v0.2.1

## v0.2.0

This is a hardfork release for the opBNB Testnet called Fermat.
It will be activated at block height 12113000, expected to occur on November 3, 2023, at 6 AM UTC.

### User Facing Changes

- Two new precompiled contracts have been introduced: blsSignatureVerify and cometBFTLightBlockValidate. The purpose of blsSignatureVerify is to verify BLS signatures in smart contracts, while cometBFTLightBlockValidate is designed to validate cometBFT light blocks. Although these contracts were primarily introduced to facilitate cross-chain communication between opBNB and Greenfield, they can also be utilized for other purposes. For instance, blsSignatureVerify can be employed to verify BLS signatures in smart contracts, and cometBFTLightBlockValidate can facilitate cross-chain communication with other blockchains based on the cosmos framework.(#7)
- Enable the layer 2 sync mechanism for opBNB by using the flag `--syncmode=snap` or `--syncmode=full` to choose the sync mode. Make sure to enable the `l2.engine-sync=true` flag on the op-node. (#8)

### Partial Changelog

- #7: feat: add precompiled contracts for Greenfield link
- #8: feat: support snap sync for OP chains
- #11: fix: pass a SnapshotOption func when init a new pruner
- #24: snap: fix snap-tests to handle legacy code lookups 
- #25: sec: update version of cometbft 

### Docker Images

- ghcr.io/bnb-chain/op-geth:v0.2.0

### Full Changelog

https://github.com/bnb-chain/op-geth/compare/v0.1.3...v0.2.0

## v0.1.3

This release adds the preDeployedContract hardfork, which changes the name and symbol of the preDeployed contract WBNB(0x4200000000000000000000000000000000000006). It also removes the preDeployed contract GovernanceToken(0x4200000000000000000000000000000000000042).

### Changelog

- ci: add docker release workflow to build and release docker image #3
- feat: add preDeployedContract hardfork #5 

## v0.1.2

This is the initial release for opBNB Testnet.

The repo base is [optimism op-geth](https://github.com/ethereum-optimism/op-geth).

### Changelog

1. [perf: concurrency and memory improvements for execution layer](https://github.com/bnb-chain/op-geth/commit/f80e72bcd2b00f738326e37c96ae150dbc9fa4d4)
2. [perf: op-node related api improvement](https://github.com/bnb-chain/op-geth/commit/0d9dc40a39242130e71621e081640935afc23a0a)
3. [feat: reannounce local pending transactions](https://github.com/bnb-chain/op-geth/pull/2)
