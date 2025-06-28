import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { Solana_Helper, Utility_Helper } from "../helpers";
import { TxType } from "../types/ERC20Tokens.types";
import { TrnxTypeEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import solProcessHelper from "./process.helper";

class queue_transactions {
    public swapProgramIds: any;

    public sol_block_tx_topic = config.KAFKA_BLOCKS.SOL_BLOCK_TX_TOPIC;
    public sol_block_tx_group_id = config.KAFKA_BLOCKS.SOL_BLOCK_TX_GROUP_ID;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

    constructor() {
        // this.swapProgramIds = ["CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY", "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB", "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv", "RVKd61ztZW9GUwhRbbLoYVRE5Xf1B2tVscKqwZqXgEr", "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS", "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1", "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c", "EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S", "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ", "DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB", "HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt", "stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq", "5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx", "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb", "BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p", "MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD", "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X", "GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn", "MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky", "Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j", "CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR", "DSwpgjMvXhtGn6BsbqmacdBZyfLj6jSWf3HJpdJtmg6N", "SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr",
        //     "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        // ]
    }


    public startBlockTransactionQueue = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {
                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: `${this.sol_block_tx_group_id}`,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: `${this.sol_block_tx_topic}`,
                    fromBeginning: false, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        const messageStartTime = Date.now();
                        try {
                            console.log(`📨 Processing message from topic: ${topic}, partition: ${partition}`);

                            const blockId = JSON.parse(message.value.toString());
                            console.log(`🔍 Processing block ID: ${blockId}`);

                            // ✅ Remove fixed delay - let the optimized parseSolanaTransaction handle timing
                            await this.parseSolanaTransaction(blockId);

                            const messageTime = Date.now() - messageStartTime;
                            console.log(`✅ Message processed successfully in ${messageTime}ms`);

                        } catch (error) {
                            const messageTime = Date.now() - messageStartTime;
                            console.error(`❌ Error processing message:`, error);
                            console.error(`⏱️  Time elapsed before error: ${messageTime}ms`);

                            // ✅ Better error recovery - don't add fixed delays
                            // The optimized parseSolanaTransaction already has proper error handling
                        }
                    },
                });

                isConnected = true;
                console.log(`✅ Consumer connected successfully to topic: ${this.sol_block_tx_topic}`);
                console.log(`📊 Consumer group ID: ${this.sol_block_tx_group_id}`);

            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // ✅ Add reconnection logic
        const startWithReconnection = async () => {
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));

                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };

    public getTxFromBlock = async (blockId: number) => {
        try {
            let blockInfo = await this.getBlockTransactions(blockId);
            if (!blockInfo) {
                console.log('blockInfo >>>', blockInfo)
                console.log(`💥 ~ do not get the block info`, `Block Number`, blockId);
            }
            if (blockInfo && blockInfo?.transactions && blockInfo?.transactions?.length > 0) {

                for await (let trnxData of blockInfo.transactions) {
                    if (trnxData.meta.err !== null) {
                        console.log('💥 ~ failed tranaction', trnxData.transaction.signatures[0]);
                        continue;
                    }
                    let txData: TxType;
                    if (trnxData?.meta?.postTokenBalances?.length > 0 && trnxData?.meta?.postTokenBalances?.[1] !== undefined) {

                        let dexExist: any = [];
                        if (trnxData?.transaction?.message?.accountKeys) {
                            dexExist = trnxData?.transaction?.message?.accountKeys?.filter((token: any) => this.swapProgramIds?.includes(token));
                        }

                        if (!dexExist.includes('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4') && !dexExist.includes('JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB') && !dexExist.includes('routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS') && !dexExist.includes('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA')) {
                            continue;
                        }

                        let index = 0;

                        for await (let i of trnxData?.meta.postTokenBalances) {
                            // if (trnxData?.meta?.postTokenBalances[index]?.mint !== undefined && trnxData?.meta?.postTokenBalances[index]?.owner == trnxData.transaction.message.accountKeys[0]) {
                            if (trnxData?.meta?.postTokenBalances[index]?.mint !== undefined) {
                                let checkToken: any = await solProcessHelper.checkIfContract(trnxData?.meta?.postTokenBalances[index].mint);
                                if (checkToken && Object.keys(checkToken)?.length > 0) {
                                    let getAmount: any = 0;
                                    const checkFilter: any = trnxData?.meta?.preTokenBalances?.find(
                                        (v: any) => v.accountIndex === trnxData?.meta?.postTokenBalances?.[index]?.accountIndex
                                    );
                                    /**  check post balance and pre balance token match on same index */
                                    if (
                                        checkFilter?.mint !== undefined &&
                                        checkFilter?.mint == trnxData?.meta?.postTokenBalances[index].mint
                                    ) {
                                        getAmount = await this.sub(
                                            Number(trnxData?.meta?.postTokenBalances[index]?.uiTokenAmount?.uiAmountString),
                                            Number(checkFilter?.uiTokenAmount?.uiAmountString)
                                        );
                                    } else {
                                        getAmount = Number(trnxData?.meta?.postTokenBalances[index]?.uiTokenAmount?.uiAmountString);
                                    }

                                    let is_withdraw = false;
                                    if (typeof getAmount == undefined) {
                                        getAmount = trnxData?.meta?.postTokenBalances[index]?.uiTokenAmount?.uiAmount;
                                    } else if (getAmount < 0 || isNaN(getAmount)) {
                                        getAmount = Math.abs(getAmount);
                                        is_withdraw = true;
                                    }

                                    let fromAddress: string = trnxData.transaction.message.accountKeys[0];
                                    let toAddress: string = trnxData?.meta?.postTokenBalances[index].owner;
                                    if (dexExist?.length > 0) {
                                        if (toAddress == fromAddress) {
                                            fromAddress = is_withdraw ? fromAddress : dexExist[0];
                                            toAddress = is_withdraw ? dexExist[0] : toAddress;
                                        } else {
                                            let fromAddress1 = fromAddress;
                                            if (is_withdraw) {
                                                fromAddress = toAddress;
                                            }
                                            if (fromAddress1 == toAddress) {
                                                toAddress = fromAddress1;
                                            }
                                        }
                                    } else {
                                        if (toAddress == fromAddress) {
                                            fromAddress = is_withdraw ? fromAddress : trnxData?.meta?.postTokenBalances[index].programId;
                                            toAddress = is_withdraw ? trnxData?.meta?.postTokenBalances[index].programId : toAddress;
                                        } else {
                                            fromAddress = is_withdraw ? toAddress : trnxData?.meta?.postTokenBalances[index].programId;
                                        }
                                    }
                                    txData = {
                                        txId: trnxData.transaction.signatures[0],
                                        fromAddress: fromAddress,
                                        toAddress: toAddress,
                                        token: checkToken,
                                        amount: Math.abs(getAmount),
                                        blockId: blockId,
                                        isNFTToken: false,
                                        status: trnxData.meta.err == null ? "success" : "failed",
                                        trnx_fee: Number(await Utility_Helper.exponentialToDecimal(
                                            await Utility_Helper.bigNumberSafeMath(trnxData.meta.fee, "/", Math.pow(10, 9))
                                        )),
                                        txType: null,
                                    };
                                    if (txData.amount !== undefined && Math.abs(txData.amount) > 0) {
                                        let ourTrnx: any = null;

                                        let withdrawTx = await solProcessHelper.getKeyValuePair(`${config.SOL_WALLET_ADDRESS}`, txData.fromAddress);
                                        if (withdrawTx) {
                                            txData.txType = TrnxTypeEnum.WITHDRAW;
                                            ourTrnx = txData;
                                        }

                                        let depositTx = await solProcessHelper.getKeyValuePair(`${config.SOL_WALLET_ADDRESS}`, txData.toAddress);
                                        if (depositTx) {
                                            txData.txType = TrnxTypeEnum.DEPOSIT;
                                            ourTrnx = txData;
                                        } else {
                                            if (is_withdraw) {
                                                txData.txType = TrnxTypeEnum.WITHDRAW;
                                            }
                                        }

                                        if (ourTrnx) {
                                            await this.addTxToQueue(
                                                ourTrnx,
                                                this.deposit_withdraw_topic || ''
                                            );
                                        }
                                    }
                                } else {
                                    console.log('💥 ~ ~ token address doesnot exist', trnxData?.meta?.postTokenBalances[index].mint)
                                }
                            }
                            index++;
                        }
                    } else {
                        txData = {
                            txId: trnxData.transaction.signatures[0],
                            fromAddress: trnxData.transaction.message.accountKeys[0],
                            toAddress: trnxData.transaction.message.accountKeys[1],
                            token: null,
                            amount: trnxData.meta.postBalances[1] - trnxData.meta.preBalances[1],
                            blockId: blockId,
                            isNFTToken: false,
                            status: trnxData.meta.err == null ? "success" : "failed",
                            trnx_fee: Number(await Utility_Helper.exponentialToDecimal(
                                await Utility_Helper.bigNumberSafeMath(trnxData.meta.fee, "/", Math.pow(10, 9))
                            )),
                            txType: null
                        };
                        if (txData.amount && txData.amount > 0) {
                            let ourTrnx: any = null;

                            let withdrawTx = await solProcessHelper.getKeyValuePair(`${config.SOL_WALLET_ADDRESS}`, txData?.fromAddress);
                            if (withdrawTx) {
                                txData.txType = TrnxTypeEnum.WITHDRAW;
                                ourTrnx = txData;
                            }

                            let depositTx = await solProcessHelper.getKeyValuePair(`${config.SOL_WALLET_ADDRESS}`, txData?.toAddress);
                            if (depositTx) {
                                txData.txType = TrnxTypeEnum.DEPOSIT;
                                ourTrnx = txData;
                            }

                            if (ourTrnx) {
                                await this.addTxToQueue(
                                    ourTrnx,
                                    this.deposit_withdraw_topic || ''
                                );
                            }
                        }
                    }
                }
            }
        } catch (error) {
            console.log("🔥 ~ ~ getTxFromBlock error", (error as Error).message);
        }
    };

    public parseSolanaTransaction = async (blockId: number) => {
        const startTime = Date.now();
        try {
            console.log(`🔍 Starting to parse block: ${blockId}`);
            let blockInfo = await this.getBlockTransactions(blockId);
            if (!blockInfo) {
                console.log('blockInfo >>>', blockInfo)
                console.log(`💥 ~ do not get the block info`, `Block Number`, blockId);
                return;
            }

            if (blockInfo && blockInfo?.transactions && blockInfo?.transactions?.length > 0) {
                const transactionCount = blockInfo.transactions.length;
                console.log(`📊 Processing ${transactionCount} transactions in block ${blockId}`);

                // Performance optimization for large blocks
                const isLargeBlock = transactionCount > 500;
                const batchSize = isLargeBlock ? 100 : 50;
                const maxConcurrency = isLargeBlock ? 10 : 5;

                if (isLargeBlock) {
                    console.log(`⚡ Large block detected (${transactionCount} transactions). Using optimized processing.`);
                    console.log(`⚙️  Batch size: ${batchSize}, Max concurrency: ${maxConcurrency}`);
                }

                // Process transactions in batches for better performance
                const batches = [];
                for (let i = 0; i < transactionCount; i += batchSize) {
                    batches.push(blockInfo.transactions.slice(i, i + batchSize));
                }

                console.log(`🔄 Processing ${batches.length} batches of ${batchSize} transactions each`);

                let processedTransactions = 0;
                // Process batches with controlled concurrency
                for (let batchIndex = 0; batchIndex < batches.length; batchIndex += maxConcurrency) {
                    const currentBatches = batches.slice(batchIndex, batchIndex + maxConcurrency);

                    await Promise.all(currentBatches.map(async (batch, batchOffset) => {
                        const actualBatchIndex = batchIndex + batchOffset;
                        const batchStartTime = Date.now();
                        // console.log(`📦 Processing batch ${actualBatchIndex + 1}/${batches.length} (${batch.length} transactions)`);

                        // Process transactions in parallel within each batch
                        await Promise.all(batch.map(async (trnxData: any) => {
                            if (trnxData.meta.err !== null) {
                                return; // Skip failed transactions
                            }

                            // Process token transactions
                            if (trnxData?.meta?.postTokenBalances?.length > 0) {
                                await this.processTokenTransaction(trnxData, blockId);
                            }

                            // Process SOL transactions
                            if (trnxData?.meta?.postBalances?.length > 0) {
                                await this.processSolTransaction(trnxData, blockId);
                            }

                            processedTransactions++;
                        }));

                        const batchTime = Date.now() - batchStartTime;
                        // console.log(`✅ Batch ${actualBatchIndex + 1} completed in ${batchTime}ms`);
                    }));

                    // Add small delay between batches for large blocks to prevent overwhelming
                    if (isLargeBlock && batchIndex + maxConcurrency < batches.length) {
                        await new Promise(resolve => setTimeout(resolve, 100));
                    }
                }

                // const totalTime = Date.now() - startTime;
                // const avgTimePerTx = totalTime / transactionCount;
                // console.log(`📈 Performance Summary for Block ${blockId}:`);
                // console.log(`   Total transactions: ${transactionCount}`);
                console.log(`   Processed transactions: ${processedTransactions}`);
                // console.log(`   Total processing time: ${totalTime}ms`);
                // console.log(`   Average time per transaction: ${avgTimePerTx.toFixed(2)}ms`);
                // console.log(`   Transactions per second: ${(transactionCount / (totalTime / 1000)).toFixed(2)}`);

            } else {
                console.log(`📭 No transactions found in block ${blockId}`);
            }
            console.log(`✅ Finished processing block: ${blockId}`);

        } catch (error) {
            const totalTime = Date.now() - startTime;
            console.error("❌ Error processing block:", error);
            console.error("Block ID that caused error:", blockId);
            console.error(`⏱️  Time elapsed before error: ${totalTime}ms`);
        }
    }

    // Optimized method for processing token transactions
    private async processTokenTransaction(trnxData: any, blockId: number) {
        try {
            const preToken = trnxData.meta.preTokenBalances || [];
            const postToken = trnxData.meta.postTokenBalances || [];
            const balancesMap: any = {};

            // Build balances map efficiently
            for (const bal of preToken) {
                const key = `${bal.mint}:${bal.owner}`;
                balancesMap[key] = {
                    mint: bal.mint,
                    owner: bal.owner,
                    pre: bal.uiTokenAmount.uiAmount ? parseFloat(bal.uiTokenAmount.uiAmount) : 0,
                    post: 0
                };
            }

            for (const bal of postToken) {
                const key = `${bal.mint}:${bal.owner}`;
                if (!balancesMap[key]) {
                    balancesMap[key] = {
                        mint: bal.mint,
                        owner: bal.owner,
                        pre: 0,
                        post: bal.uiTokenAmount.uiAmount ? parseFloat(bal.uiTokenAmount.uiAmount) : 0
                    };
                } else {
                    balancesMap[key].post = parseFloat(bal.uiTokenAmount.uiAmount);
                }
            }

            const mints: Set<string> = new Set(Object.values(balancesMap).map((b: any) => b.mint));
            const uniqueMints = Array.from(mints).filter(mint => mint);

            // Batch process mints for better performance
            const mintBatches = [];
            for (let i = 0; i < uniqueMints.length; i += 10) {
                mintBatches.push(uniqueMints.slice(i, i + 10));
            }

            for (const mintBatch of mintBatches) {
                await Promise.all(mintBatch.map(async (mint: string) => {
                    const checkToken: any = await solProcessHelper.checkIfContract(mint);
                    if (!checkToken || Object.keys(checkToken)?.length === 0) return;

                    interface BalanceItem {
                        mint: string;
                        owner: string;
                        pre: number;
                        post: number;
                    }

                    const deltas: BalanceItem[] = Object.values(balancesMap)
                        .filter((b: any) => b.mint === mint)
                        .map((b: any) => ({
                            mint: b.mint,
                            owner: b.owner,
                            pre: b.pre,
                            post: b.post
                        }));

                    const senders: BalanceItem[] = deltas.filter((b) => b.post < b.pre);
                    const receivers: BalanceItem[] = deltas.filter((b) => b.post > b.pre);

                    // Process sender-receiver pairs efficiently
                    const txPromises = [];
                    for (const sender of senders) {
                        for (const receiver of receivers) {
                            const amount = Math.min(sender.pre - sender.post, receiver.post - receiver.pre);
                            if (amount <= 0) continue;

                            const txData: TxType = {
                                txId: trnxData.transaction.signatures[0],
                                fromAddress: sender.owner,
                                toAddress: receiver.owner,
                                token: checkToken,
                                amount: amount,
                                blockId: blockId,
                                isNFTToken: false,
                                status: trnxData.meta.err == null ? "success" : "failed",
                                trnx_fee: Number(await Utility_Helper.exponentialToDecimal(
                                    await Utility_Helper.bigNumberSafeMath(trnxData.meta.fee, "/", Math.pow(10, 9))
                                )),
                                txType: null,
                            };

                            // Batch Redis calls for better performance
                            const addresses: string[] = [];
                            if (txData.fromAddress) addresses.push(txData.fromAddress);
                            if (txData.toAddress) addresses.push(txData.toAddress);

                            if (addresses.length > 0) {
                                const results = await solProcessHelper.getMultipleKeyValuePairs(`${config.SOL_WALLET_ADDRESS}`, addresses);
                                if (results) {
                                    const [withdrawTx, depositTx] = results as string[];
                                    if (depositTx) {
                                        txData.txType = TrnxTypeEnum.DEPOSIT;
                                    } else if (withdrawTx) {
                                        txData.txType = TrnxTypeEnum.WITHDRAW;
                                    }
                                }
                            }

                            if (txData.txType !== null) {
                                txPromises.push(this.addTxToQueue(txData, this.deposit_withdraw_topic || ''));
                            }

                            sender.post += amount;
                            receiver.pre += amount;
                        }
                    }

                    // Execute all transaction promises
                    if (txPromises.length > 0) {
                        await Promise.all(txPromises);
                    }
                }));
            }
        } catch (error) {
            console.error("❌ Error processing token transaction:", error);
        }
    }

    // Optimized method for processing SOL transactions
    private async processSolTransaction(trnxData: any, blockId: number) {
        try {
            const accountKeys = trnxData.transaction.message.accountKeys;
            const preBalances = trnxData.meta.preBalances;
            const postBalances = trnxData.meta.postBalances;

            const solDelta = [];
            for (let i = 0; i < preBalances.length; i++) {
                const delta = postBalances[i] - preBalances[i];
                if (delta !== 0) {
                    solDelta.push({
                        address: accountKeys[i]?.pubkey || accountKeys[i],
                        change: delta
                    });
                }
            }

            if (solDelta.length > 0) {
                const senders = solDelta.filter(d => d.change < 0);
                const receivers = solDelta.filter(d => d.change > 0);

                const txPromises = [];
                for (const sender of senders) {
                    for (const receiver of receivers) {
                        const amount = Math.min(-sender.change, receiver.change);
                        if (amount > 0) {
                            const txData: TxType = {
                                txId: trnxData.transaction.signatures[0],
                                fromAddress: sender.address,
                                toAddress: receiver.address,
                                token: null,
                                amount: amount,
                                blockId: blockId,
                                isNFTToken: false,
                                status: trnxData.meta.err == null ? "success" : "failed",
                                trnx_fee: Number(await Utility_Helper.exponentialToDecimal(
                                    await Utility_Helper.bigNumberSafeMath(trnxData.meta.fee, "/", Math.pow(10, 9))
                                )),
                                txType: null
                            };

                            sender.change += amount;
                            receiver.change -= amount;

                            const addresses: string[] = [];
                            if (txData.fromAddress) addresses.push(txData.fromAddress);
                            if (txData.toAddress) addresses.push(txData.toAddress);

                            if (addresses.length > 0) {
                                const results = await solProcessHelper.getMultipleKeyValuePairs(`${config.SOL_WALLET_ADDRESS}`, addresses);
                                if (results) {
                                    const [withdrawTx, depositTx] = results as string[];
                                    if (depositTx) {
                                        txData.txType = TrnxTypeEnum.DEPOSIT;
                                    } else if (withdrawTx) {
                                        txData.txType = TrnxTypeEnum.WITHDRAW;
                                    }
                                }
                            }

                            if (txData.txType !== null) {
                                txPromises.push(this.addTxToQueue(txData, this.deposit_withdraw_topic || ''));
                            }
                        }
                    }
                }

                // Execute all transaction promises
                if (txPromises.length > 0) {
                    await Promise.all(txPromises);
                }
            }
        } catch (error) {
            console.error("❌ Error processing SOL transaction:", error);
        }
    }

    public async checkAddressInDB(txData: TxType) {
        try {
            let ourTrnx: any = null;
            let withdraw = await solProcessHelper.check_our_wallet_address(txData.fromAddress);
            if (withdraw) {
                console.log("Found the user in FROM (WITHDRAW) tx 😸");
                txData.txType = TrnxTypeEnum.WITHDRAW;
                ourTrnx = txData;
            }
            let deposit = await solProcessHelper.check_our_wallet_address(txData.toAddress);
            if (deposit) {
                console.log("Found the user in to (DEPOSIT) tx 😸");
                txData.txType = TrnxTypeEnum.DEPOSIT;
                ourTrnx = txData;
            }
            if (ourTrnx) {
                await sol_tx_status_deposit_withdraw.addTxToQueue(
                    ourTrnx,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }

    public async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await global_helper.save_error_logs('Sol_checkAddressInDB', err.message)

        }
    };

    private getBlockTransactions = async (blockNo: number) => {
        try {
            let rpcJson = {
                jsonrpc: "2.0",
                id: 1,
                method: "getBlock",
                params: [
                    +blockNo,
                    {
                        encoding: "json",
                        transactionDetails: "full",
                        rewards: false,
                        maxSupportedTransactionVersion: 0
                    },
                ],
            };
            return await Solana_Helper
                .rpcCall(rpcJson)
                .then((result) => {
                    return result;
                })
                .catch(async (error) => {
                    console.log("🔥 ~ ~ getBlockTransactions rpcCall error", error);
                    return await Solana_Helper
                        .rpcCallPublicNode(rpcJson)
                        .then((result) => { return result; })
                        .catch((err) => {
                            console.log("💥 ~ ~ getBlockTransactions Solana_Helper.rpcCallPublicNode error", err?.message || err);
                            return false;
                        });
                    // return false;
                });
        } catch (err: any) {
            console.log("🔥 ~ ~ getBlockTransactions error", err);
            return err.message;
        }
    };

    private sub = async (b: number, c: number) => {
        try {
            let b1 = b.toString().split(".");
            let b1_max = 0;
            if (b1.length == 2) {
                b1_max = b1[1].length;
            }
            let c1 = c.toString().split(".");
            let c1_max = 0;
            if (c1.length == 2) {
                c1_max = c1[1].length;
            }
            let max_len = b1_max > c1_max ? b1_max : c1_max;
            return Number((b - c).toFixed(max_len));
        } catch (error) {
            console.log('💥 ~ ~ sub error', (error as Error).message);
        }
    };

    private sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

    // ✅ Add method to reset consumer offset for reprocessing
    public resetConsumerOffset = async (topic: string, partition: number = 0) => {
        try {
            console.log(`🔄 Resetting consumer offset for topic: ${topic}, partition: ${partition}`);

            const consumer = await kafkaHelper.kafka.consumer({
                groupId: `${this.sol_block_tx_group_id}`,
            });

            await consumer.connect();

            // Reset offset to beginning
            await consumer.seek({
                topic: topic,
                partition: partition,
                offset: '0' // Start from the beginning
            });

            console.log(`✅ Consumer offset reset successfully`);
            await consumer.disconnect();

        } catch (error) {
            console.error(`❌ Error resetting consumer offset:`, error);
            throw error;
        }
    };

    // ✅ Add method to get consumer status
    public getConsumerStatus = async () => {
        try {
            const consumer = await kafkaHelper.kafka.consumer({
                groupId: `${this.sol_block_tx_group_id}`,
            });

            await consumer.connect();

            // Get consumer group description
            const admin = kafkaHelper.kafka.admin();
            await admin.connect();

            const groupDescription = await admin.describeGroups([this.sol_block_tx_group_id]);
            await admin.disconnect();

            console.log(`📊 Consumer Group Status:`, groupDescription);
            await consumer.disconnect();

            return groupDescription;

        } catch (error) {
            console.error(`❌ Error getting consumer status:`, error);
            return null;
        }
    };

}
export const sol_tx_status_deposit_withdraw = new queue_transactions()
