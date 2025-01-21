# Release Note \- TxDAG based parallel transaction execution (PEVM) \- Alpha

## Version

**Alpha - Not production ready**

**Based on op-geth v0.5.1** 

## Description

This is the first release of the op-geth that implements the TxDAG based parallel transaction execution for block synchronization. The goal is to speed up the execution of transactions in blocks by exploiting parallelism in EVM, and improve the performance further with the TxDAG data guidance by reducing the re-execution caused by transaction dependencies.

### Release goals

* Provide the parallel transaction execution implementation that is 100% compatible with the original EVM.  
* Provide the raw TxDAG data in file format as a POC for TxDAG guided PEVM execution  
* Provide the op-geth binary release that is verified to successfully sync the block from genesis to block \#15000000 with TxDAG and PEVM enabled, and with performance improved in some scenarios and no obvious performance drop overall.  
* Provide the methodology to enable and use the TxDAG base PEVM for block syncing of opBNB.  
* Provide the preliminary performance data and analysis for further improvement work

### Options Added

*    --parallel                          (default: false)                   ($GETH\_PARALLEL)

          Enable the experimental parallel transaction execution mode, only valid in full  
          sync mode (default = false)  
   

*    --parallel.num value                (default: 0\)                       ($GETH\_PARALLEL\_NUM)

          Number of slot for transaction execution, only valid in parallel mode (runtime  
          calculated, no fixed default value)  
   

*    --parallel.txdag                    (default: false)                   ($GETH\_PARALLEL\_TXDAG)

          Enable the experimental parallel TxDAG generation, only valid in full sync mode  
          (default = false)  
   

*    --parallel.txdagfile value          (default: "./parallel-txdag-output.csv") ($GETH\_PARALLEL\_TXDAGFILE)

          It indicates the TxDAG file path  


*    --parallel.unordered-merge          (default: false)                   ($GETH\_PARALLEL\_UNORDERED\_MERGE)

          Enable unordered merge mode, during the parallel confirm phase, merge  
          transaction execution results without following the transaction order.  
   

### Usage

* Get the TxDAG data:  
  * User could download the TxDAG data from [https://pub-c0627345c16f47ab858c9469133073a8.r2.dev/opbnb-txdag/parallel-txdag-output\_compare\_15000000.csv](https://pub-c0627345c16f47ab858c9469133073a8.r2.dev/opbnb-txdag/parallel-txdag-output_compare_15000000.csv)  
* Use the following options of the geth to enable TxDAG based parallel execution  
  * '--parallel'
  * '--parallel.txtag'

		Enable the TxDAG functionality of geth, this will tell EVM to check the TxDAG data existence before block execution

  * '--parallel.txdagfile=./parallel-txdag-output\_compare\_15000000.csv'  

        Specify the path of the TxDAG data file 

  * '--parallel.unordered-merge'  
        
        Trust the DAG data, enable the advanced unordered-merge optimization and skip conflict check.   
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
  --parallel.unordered-merge \  
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

### Performance

| Scenario    | Description                                                                                                 | mgasps<br/>(Original) | mgasps<br/>(PEVM) | Improvement | Comments                                                 |
|-------------|-------------------------------------------------------------------------------------------------------------|----------------------|--------------|-------------|----------------------------------------------------------|
| **A**       | **Internal test chain blocks with 250k accounts transfer to another 250k account**                          | **107**              | **202**      | **88%**     | **conflict rate(avg): ~0%<br/>txs in block(avg): 3103**  |
| **B**       | **Internal test chain blocks with 250k accounts transfer to 1 fixed account**                               | **201**              | **216**      | **7%**      | **conflict rate(avg): ~95%<br/>txs in block(avg): 3644** |
| **C**       | **Internal test chain blocks contain random selected txs with a mix of smart contract and native transfer** | **155**              | **228**      | **47%**     | **conflict rate(avg): ~81%<br/>txs in block(avg): 1356** |
| **Mainnet** | **opBNB mainnet block range from #9m-9.3m**                                                                 | **9.3**              | **11.2**     | **20%**     | **conflict rate(avg): ~12%<br/>txs in block(avg): 25**   |
| **Mainnet** | **opBNB mainnet block range from #11.9m-12.1m (mostly inscription txs)**                                    | **57**               | **61.1**     | **7%**      | **conflict rate(avg): ~50%<br/>txs in block(avg): 2195** |

### Additional Info

Conclusions for alpha release

* The performance of PEVM is highly depend on scenarios and dependencies between txs in block  
* Generally no obvious degradation of performance observed on mainnet sync (from block#1 to #15000000)

#### TxDAG data solution

The current file-based TxDAG data solution will be optimized with an alternative gasless transaction based
TxDAG transfer in the future. It will guarantee that the TxDAG data will be available with the block transactions
as soon as the block is generated. 
More details in [BEP-396: Accelerate Block Execution by TxDAG](https://forum.bnbchain.org/t/bep-396-accelerate-block-execution-by-txdag/2869)