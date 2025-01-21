# Release Note \- TxDAG based parallel transaction execution (PEVM) \- Beta

## Version

**Beta - Not production ready**

**Based on op-geth v0.5.4** 

## Description

This is the beta release of the op-geth that implements the TxDAG based parallel transaction execution for block synchronization. The goal is to speed up the execution of transactions in blocks by exploiting parallelism in EVM, and improve the performance further with the TxDAG data guidance by reducing the re-execution caused by transaction dependencies.

### Release goals

* Provide the parallel transaction execution implementation that is 100% compatible with the original EVM.  
* Provide the ability to load TxDAG data from on-chain blocks or files  
* Provide the op-geth binary release that is verified to successfully sync the block from genesis to latest block with TxDAG and PEVM enabled, and with performance improved in some scenarios and no obvious performance drop overall.

### Note

* The opBNB mainnet and testnet have already contains the TxDAG data on chain start from following block:
  * opBNB mainnet: block@43274759
  * opBNB testnet: block@47121260
* For TxDAG data in previous blocks from genesis, user could download the DAG files with following link:
  * opBNB mainnet (block 0-43274759) [TxDAG-File](https://pub-c0627345c16f47ab858c9469133073a8.r2.dev/opbnb-txdag/mainnet_final_43274759.csv)
  * opBNB testnet (block 0-47121260) [TxDAG-File](https://pub-c0627345c16f47ab858c9469133073a8.r2.dev/opbnb-txdag/testnet_final_47121260.csv)
### Options Added

*    --parallel.parallel-merge           (default: false)                  

          Enable concurrent merge mode, during the parallel confirm phase, multiple
          goroutines will be used to perform concurrent merging of execution results. This
          option will override parallel.unordered-merge

*    --parallel.txdag-max-depth-ratio value (default: 0.9)                  

         A ratio to decide whether or not to execute transactions in parallel, it will
         fallback to sequencial processor if the depth is larger than this value (default
         = 0.9)



### Usage

* (optional) Get the TxDAG data:  
  * User could download the TxDAG data for opBNB mainnet or testnet
* Use the following options of the geth to enable TxDAG based parallel execution  
  * '--parallel'
  * '--parallel.txtag'

		Enable the TxDAG functionality of geth, this will tell EVM to check the TxDAG data existence before block execution

  * (optional) '--parallel.txdagfile=./mainnet\_final\_43274759.csv'  

        Optional, Specify the path of the TxDAG data file 

  * '--parallel.parallel-merge'  
        
        Trust the DAG data, enable the advanced parallel merge optimization and skip conflict check.   
  * '--parallel.num=4'

        Optional, Specify the parallel execution number, if not assigned, use the core number for the running platform   
    

### Example
```
op-geth \  
  --datadir="./datadir" \  
  --verbosity=4 \  
  --http \ 
  --http.corsdomain="*" \  
  --http.vhosts="*" \  
  --http.addr=0.0.0.0 \  
  --http.port=8545 \  
  --http.api=net,eth,engine,debug \  
  --pprof \  
  --pprof.port=6070 \  
  --ws \  
  --ws.addr=0.0.0.0 \  
  --ws.port=8545 \  
  --ws.origins="*" \  
  --ws.api=eth,engine \  
  --syncmode=full \  
  --networkid=$CHAIN_ID \  
  --txpool.globalslots=20000 \  
  --txpool.globalqueue=5000 \  
  --txpool.accountqueue=200 \  
  --txpool.accountslots=200 \  
  --txpool.nolocals=true \  
  --txpool.pricelimit=1 \   
  --cache.preimages \  
  --allow-insecure-unlock \  
  --authrpc.addr="0.0.0.0" \  
  --authrpc.port="8551" \  
  --authrpc.vhosts="*" \  
  --authrpc.jwtsecret=./jwt.txt \  
  --rpc.allow-unprotected-txs \  
  --parallel \  
  --parallel.txdag \  
  --parallel.txdagfile=./parallel-txdag-output.csv \  
  --parallel.parallel-merge \  
  --parallel.num=4 \  
  --gcmode=full \  
  --metrics \  
  --metrics.port 6068 \  
  --metrics.addr 0.0.0.0 \  
  --rollup.sequencerhttp=$L2_RPC \  
  --rollup.disabletxpoolgossip=false \  
  --bootnodes=$P2P_BOOTNODES
```

### 

### Additional Info

Conclusions for alpha release

* The performance of PEVM is highly depend on scenarios and dependencies between txs in block  
* Generally no obvious degradation of performance observed on mainnet sync

#### TxDAG data solution

Starting from this release, the TxDAG data file is finalized and will not update in the future.
Now The TxDAG is generated and propagated in blocks on chain, so it guarantee that the TxDAG data will be available with the block transactions
as soon as the block is generated. 
More details in [BEP-396: Accelerate Block Execution by TxDAG](https://forum.bnbchain.org/t/bep-396-accelerate-block-execution-by-txdag/2869)