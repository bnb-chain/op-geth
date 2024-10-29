# Changelog

## v0.5.1

This release includes various optimizations and improvements to transaction processing, CI support, and network infrastructure.

This is a minor release for opBNB Mainnet and Testnet.
Upgrading is optional.

### What's Changed

* fix(ci): support building arm64 architecture (#165)
* optimization: enqueue transactions in parallel from p2p (#173)
* optimization: enlarge p2p buffer size and add some metrics for performance monitor (#171)
* optimization: txpool pricedlist only reheap when pool is full (#175)
* optimization: txpool pending cache improvement (#177)
* chore: add bootnode in us region(testnet) (#194)

### Docker Images
ghcr.io/bnb-chain/op-geth:v0.5.1

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.5.0...v0.5.1

## v0.5.0
This release includes code merging from the upstream version v1.101315.2 along with several fixs and improvements. Fjord fork from upstream is included.
Fjord fork is scheduled to launch on the opBNB: 
Testnet: Sep-10-2024 06:00 AM +UTC
Mainnet: Sep-24-2024 06:00 AM +UTC

### User Facing Changes
Nodes on the Testnet and Mainnet need to be upgraded to this version before the hard fork time.

### What's Changed
* Merge upstream op-geth v1.101315.2 by @redhdx in https://github.com/bnb-chain/op-geth/pull/123
* feature(op-geth): update opBNB qanet config by @redhdx in https://github.com/bnb-chain/op-geth/pull/138
* fix: txpool legacy pool Pending() applies filter @andyzhang2023 in https://github.com/bnb-chain/op-geth/pull/124
* feature(op-geth): Add extra error info when meet an unexpected el sync @krish-nr in https://github.com/bnb-chain/op-geth/pull/132
* feat: cache lastTail in txindexer to avoid read from db by @bnoieh in https://github.com/bnb-chain/op-geth/pull/157


### Docker Images
ghcr.io/bnb-chain/op-geth:v0.5.0

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.4.6...v0.5.0

## v0.4.6

This is a minor release for opBNB Mainnet and Testnet.
Upgrading is optional.

### What's Changed
* perf: add DialOptions function for Dial by @constwz in https://github.com/bnb-chain/op-geth/pull/140
* Fix: clear difflayer cache when truncate not triggered by @krish-nr in https://github.com/bnb-chain/op-geth/pull/141
* fix addBundle issue by @redhdx in https://github.com/bnb-chain/op-geth/pull/143

### New Contributors
* @constwz made their first contribution in https://github.com/bnb-chain/op-geth/pull/140

### Docker Images

- ghcr.io/bnb-chain/op-geth:v0.4.6

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.4.5...v0.4.6

## v0.4.5

This is a hard fork release for the opBNB Mainnet called **Wright**. It will be activated on August 27, 2024, at 6 AM UTC.
All **mainnet** op-geth nodes must upgrade to this release before the hard fork.

The testnet hardfork will be activated on August 15, 2024, at 6 AM UTC.
If you have upgraded your testnet op-geth to version 0.4.4, you can skip this version for the testnet. Otherwise, you can directly upgrade your testnet op-geth to version 0.4.5.

### User Facing Changes

To support gasless transactions on opBNB, the following features have been introduced:

* The base fee is set to 0.
* The bundle feature is supported.
* When the gas price is set to 0, the L1 fee will also be set to 0.

Combined with these features and a sponsor (paymaster), users can send transactions without holding BNB to pay gas fees.

### Changelogs
* perf: speedup pbss trienode read by @will-2012 in https://github.com/bnb-chain/op-geth/pull/122
* pathdb: handle persistent id when using nodebufferlist by @sysvm in https://github.com/bnb-chain/op-geth/pull/121
* feat: support auto recover when pbss meet unclean shutdown by @krish-nr in https://github.com/bnb-chain/op-geth/pull/125
* fix: ignore truncation target range as flush not operated on time by @krish-nr in https://github.com/bnb-chain/op-geth/pull/131
* feature(op-geth): add opbnb gasless solution by @redhdx in https://github.com/bnb-chain/op-geth/pull/130

### Docker Images

- ghcr.io/bnb-chain/op-geth:v0.4.5

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.4.3...v0.4.5

## v0.4.4

This is a hard fork release for the opBNB Testnet called **Wright**. It will be activated on August 15, 2024, at 6 AM UTC.
All **testnet** nodes must upgrade to this release before the hard fork.

Upgrading for mainnet nodes is optional.

### User Facing Changes

To support gasless transactions on opBNB, the following features have been introduced:

* The base fee is set to 0.
* The bundle feature is supported.
* When the gas price is set to 0, the L1 fee will also be set to 0.

Combined with these features and a sponsor (paymaster), users can send transactions without holding BNB to pay gas fees.

### Changelogs
* perf: speedup pbss trienode read by @will-2012 in https://github.com/bnb-chain/op-geth/pull/122
* pathdb: handle persistent id when using nodebufferlist by @sysvm in https://github.com/bnb-chain/op-geth/pull/121
* feat: support auto recover when pbss meet unclean shutdown by @krish-nr in https://github.com/bnb-chain/op-geth/pull/125
* fix: ignore truncation target range as flush not operated on time by @krish-nr in https://github.com/bnb-chain/op-geth/pull/131
* feature(op-geth): add opbnb gasless solution by @redhdx in https://github.com/bnb-chain/op-geth/pull/130

### Docker Images

- ghcr.io/bnb-chain/op-geth:v0.4.4

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.4.3...v0.4.4

## 0.4.3

This is a minor release for opBNB Mainnet and Testnet.
Upgrading is optional.

### User Facing Changes

* Conducted several performance improvements for txpool and block execution.(#89, #84, #85, #92)
* Added `--journalFile` flag to enable/disable the journal file feature. #95

### What's Changed
* feat(txpool): improve performance of Reheap by @andyzhang2023 in https://github.com/bnb-chain/op-geth/pull/89
* feat(txpool): improve demotion unexecutable transactions by @andyzhang2023 in https://github.com/bnb-chain/op-geth/pull/84
* opt: do verify and commit concurrently by @joeylichang in https://github.com/bnb-chain/op-geth/pull/92
* improve Pending() of txpool to reduce the latency when miner worker committing transactions by @andyzhang2023 in https://github.com/bnb-chain/op-geth/pull/85
* core/trie: persist TrieJournal to journal file instead of kv database by @sysvm in https://github.com/bnb-chain/op-geth/pull/95
* add some metrics for txpool by @andyzhang2023 in https://github.com/bnb-chain/op-geth/pull/120

## Docker Images

ghcr.io/bnb-chain/op-geth:v0.4.3

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.4.2...v0.4.3

## 0.4.2

This is the mainnet hardfork release version.

Four hard forks are scheduled to launch on the opBNB Mainnet:
Shanghai/Canyon Time: 2024-06-20 08:00:00 AM UTC
Delta Time: 2024-06-20 08:10:00 AM UTC
Cancun/Ecotone Time: 2024-06-20 08:20:00 AM UTC
Haber Time: 2024-06-20 08:30:00 AM UTC

All mainnet `op-geth` have to be upgraded to this version before  2024-06-20 08:00:00 AM UTC.
The `op-node` also have to be upgraded to v0.4.2 accordingly, check [this](https://github.com/bnb-chain/opbnb/releases/tag/v0.4.2) for more details.

### What's Changed
* fix: adjust flush condition to avoid not flush by @will-2012 in https://github.com/bnb-chain/op-geth/pull/114
* config: Mainnet Shanghai/Canyon/Cancun/Ecotone/Haber fork time by @welkin22 in https://github.com/bnb-chain/op-geth/pull/116

### Docker Images

ghcr.io/bnb-chain/op-geth:v0.4.2

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.4.1...v0.4.2

## 0.4.1

This is the Haber Hardfork release for opBNB Testnet.

The Haber hardfork will be activated on May 30, 2024, at 6:00 AM UTC.

All testnet nodes must upgrade to this release before the hardfork.
Upgrading for other networks is optional.

### User Facing Changes
* Introduced precompile for secp256r1 curve as [EIP 7212](https://eips.ethereum.org/EIPS/eip-7212). Please refer to this [PR](https://github.com/bnb-chain/op-geth/pull/112) for more details.

### What's Changed
* cmd/dbcmd: add inspect trie tool by @sysvm in https://github.com/bnb-chain/op-geth/pull/103
* fix: cherry-picked from upstream code by @welkin22 in https://github.com/bnb-chain/op-geth/pull/109
* fix: add miss cache code after 4844 merge by @welkin22 in https://github.com/bnb-chain/op-geth/pull/110
* feat: add proof keeper by @will-2012 in https://github.com/bnb-chain/op-geth/pull/90
* feature(op-geth): add precompile for secp256r1 curve after haber hardfork by @redhdx in https://github.com/bnb-chain/op-geth/pull/112

### Docker Images
* ghcr.io/bnb-chain/op-geth:v0.4.1

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.4.0...v0.4.1

## 0.4.0
This release includes code merging from the upstream version v1.101308.2 to transition Testnet's DA data from calldata to blob format.
Four hard forks are scheduled to launch on the opBNB Testnet:
Snow Time: May-15-2024 06:00 AM +UTC
Shanghai/Canyon Time: May-15-2024 06:10 AM +UTC
Delta Time: May-15-2024 06:20 AM +UTC
Cancun/Ecotone Time: May-15-2024 06:30 AM +UTC

The Shanghai/Canyon fork and Cancun/Ecotone fork involve changes to op-geth.

### User Facing Changes
Nodes on the **Testnet** need to be upgraded to this version before the first hard fork time.
**Note: This is a version prepared for Testnet, Mainnet nodes do not need to upgrade to this version.**

### Breaking Changes
Fast Node Mode (released in version [v0.3.1](https://github.com/bnb-chain/op-geth/releases/tag/v0.3.1)) has become unusable due to conflicts with upstream code.
We are working to resolve this issue and expect it to be fixed in the next version.

### What's Changed
* feature(op-geth): update opBNB qanet config by @redhdx in https://github.com/bnb-chain/op-geth/pull/99
* canyon fork: revert disable create2deployer in canyon fork by @bnoieh in https://github.com/bnb-chain/op-geth/pull/100
* feature(op-geth): add opBNB qanet hard fork config by @redhdx in https://github.com/bnb-chain/op-geth/pull/101
* add precompiled contracts for light client by @KeefeL in https://github.com/bnb-chain/op-geth/pull/102
* feature: add opBNB V5 bootnodes by @redhdx in https://github.com/bnb-chain/op-geth/pull/104
* add bls example by @KeefeL in https://github.com/bnb-chain/op-geth/pull/105
* Merge upstream op-geth v1.101308.2 by @welkin22 in https://github.com/bnb-chain/op-geth/pull/87
* fix: the TrieCommitInterval not taking effect on restart by @welkin22 in https://github.com/bnb-chain/op-geth/pull/106
* config: Testnet 4844 fork time by @welkin22 in https://github.com/bnb-chain/op-geth/pull/107

### Docker Images
ghcr.io/bnb-chain/op-geth:v0.4.0

**Full Changelog**: https://github.com/bnb-chain/op-geth/compare/v0.3.1...v0.4.0

## v0.3.1

This is a minor release for opBNB Mainnet and Testnet.

In this release, we've introduced significant optimizations and enhancements across various modes and functionalities, including PBSS Mode optimizations, the introduction of Fast Node Mode, EVM execution improvements, and a simplified start command, aimed at enhancing performance and user experience.

Upgrading is optional.

### User Facing Changes

- **PBSS Mode Optimizations**: We have made multiple optimizations for PBSS mode, including the implementation of an async buffer, support for obtaining withdrawal proof on PBSS, and the introduction of an HBSS to PBSS convert tool. For more details, please refer to #69, #74, and #79.
- **Fast Node Mode**: We have introduced a new mode called "Fast Node" to improve the performance and reduce the resources required by the node. This enhancement is aimed at providing a more efficient and streamlined experience. Please see more details in #75.
- **EVM Execution Opcode Level Optimization**: Our latest update includes optimizations at the EVM execution opcode level, resulting in improved performance for EVM execution. For a deeper understanding of these changes, please refer to #77 and #81.
- **Simplified Start Command**: We have simplified the start command by enabling the selection of default network configuration through the use of `--opBNBMainnet` or `--opBNBTestnet` in the command. This enhancement aims to streamline the user experience. More information can be found in #91.

### Partial Changelog

- [#69](https://github.com/bnb-chain/op-geth/pull/69): feat: add async buffer
- [#71](https://github.com/bnb-chain/op-geth/pull/71): chore: add miner perf metrics
- [#74](https://github.com/bnb-chain/op-geth/pull/74): feat: support getting withdrawal proof on pbss
- [#75](https://github.com/bnb-chain/op-geth/pull/75): feat: implement fast node
- [#76](https://github.com/bnb-chain/op-geth/pull/76): bugfix: fix Resubscribe deadlock when unsubscribing after inner sub ends
- [#77](https://github.com/bnb-chain/op-geth/pull/77): feat: EVM execution opcode level optimization
- [#79](https://github.com/bnb-chain/op-geth/pull/79): cmd/geth: add hbss to pbss convert tool
- [#81](https://github.com/bnb-chain/op-geth/pull/81): feat: introduce hash cache to improve performance
- [#88](https://github.com/bnb-chain/op-geth/pull/88): fix: fix base buffer concurrent read/write race
- [#91](https://github.com/bnb-chain/op-geth/pull/91): feat: simplify node start

### Docker Images

- ghcr.io/bnb-chain/op-geth:v0.3.1

### Full Changelog

https://github.com/bnb-chain/op-geth/compare/v0.3.0...v0.3.1

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
