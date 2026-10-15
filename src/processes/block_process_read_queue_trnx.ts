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
        this.swapProgramIds = ["CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY", "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB", "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv", "RVKd61ztZW9GUwhRbbLoYVRE5Xf1B2tVscKqwZqXgEr", "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS", "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1", "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c", "EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S", "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ", "DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB", "HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt", "stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq", "5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx", "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb", "BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p", "MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD", "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X", "GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn", "MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky", "Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j", "CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR", "DSwpgjMvXhtGn6BsbqmacdBZyfLj6jSWf3HJpdJtmg6N", "SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
        ]
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
                    groupId: `${this.sol_block_tx_group_id}_a`,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: `${this.sol_block_tx_topic}_a`,
                    fromBeginning: true, // ✅ Changed to false to prevent reprocessing
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
                            await new Promise(resolve => setTimeout(resolve, 400));

                            // ✅ Remove fixed delay - let the optimized parseSolanaTransaction handle timing
                            this.parseSolanaTransaction(blockId);

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
    public startBlockTransactionQueueB = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {
                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: `${this.sol_block_tx_group_id}_b`,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: `${this.sol_block_tx_topic}_b`,
                    fromBeginning: true, // ✅ Changed to false to prevent reprocessing
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
                            await new Promise(resolve => setTimeout(resolve, 350));
                            // ✅ Remove fixed delay - let the optimized parseSolanaTransaction handle timing
                            this.parseSolanaTransaction(blockId);

                            const messageTime = Date.now() - messageStartTime;
                            console.log(`✅ ${topic} Message processed successfully in ${messageTime}ms`);

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


    public parseSolanaTransaction = async (blockId: number) => {
        try {
            let blockInfo = await this.getBlockTransactions(blockId);
            if (!blockInfo) {
                console.log('blockInfo >>>', blockInfo)
                console.log(`💥 ~ do not get the block info`, `Block Number`, blockId);
                return;
            }
            if (blockInfo && blockInfo?.transactions && blockInfo?.transactions?.length > 0) {
                for await (let trnxData of blockInfo.transactions) {
                    if (trnxData.meta.err !== null) {
                        continue;
                    }
                    const accountKeys = trnxData?.transaction?.message?.accountKeys || [];


                    const signature = trnxData.transaction.signatures[0];

                    /** for Sol Token Transactions */
                    if (trnxData?.meta?.postTokenBalances?.length > 0 && trnxData?.meta?.postTokenBalances?.[1] !== undefined) {
                        const dexExist = accountKeys.filter((token: any) => this.swapProgramIds?.includes(token));

                        // Skip transactions that aren't related to relevant swap programs
                        const validDexProgram = [
                            'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
                            'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',
                            'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS',
                            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                            'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
                        ];

                        if (!dexExist.some((id: any) => validDexProgram.includes(id))) continue;

                        const preToken = trnxData.meta.preTokenBalances || [];
                        const postToken = trnxData.meta.postTokenBalances || [];
                        const balancesMap: Record<string, any> = {};

                        // Process pre-token balances without await
                        for (const bal of preToken) {
                            const key = `${bal.mint}:${bal.owner}`;
                            const perAmount =
                                typeof bal.uiTokenAmount.uiAmountString === 'number' || typeof bal.uiTokenAmount.uiAmountString === 'string'
                                    ? parseFloat(bal.uiTokenAmount.uiAmountString as string)
                                    : 0;
                            balancesMap[key] = {
                                mint: bal.mint,
                                owner: bal.owner,
                                pre: perAmount,
                                post: 0
                            };
                        }
                        // Process post-token balances without await
                        for (const bal of postToken) {
                            const key = `${bal.mint}:${bal.owner}`;

                            const postAmount =
                                typeof bal.uiTokenAmount.uiAmountString === 'number' || typeof bal.uiTokenAmount.uiAmountString === 'string'
                                    ? parseFloat(bal.uiTokenAmount.uiAmountString as string)
                                    : 0;
                            if (!balancesMap[key]) {
                                balancesMap[key] = {
                                    mint: bal.mint,
                                    owner: bal.owner,
                                    pre: 0,
                                    post: postAmount
                                };
                            } else {
                                balancesMap[key].post = postAmount;
                            }
                        };

                        const mints: Set<string> = new Set(Object.values(balancesMap).map((b: any) => b.mint));
                        // Process all mints in parallel
                        await Promise.all(Array.from(mints).map(async (mint: string) => {
                            if (!mint) return;

                            const checkToken: any = await solProcessHelper.checkIfContract(mint);
                            if (!checkToken || Object.keys(checkToken)?.length === 0) return;

                            interface BalanceItem {
                                mint: string;
                                owner: string;
                                pre: number;
                                post: number;
                            }

                            const deltas: BalanceItem[] = Object.values(balancesMap).filter((b: any) => b.mint === mint).map((b: any) => ({
                                mint: b.mint,
                                owner: b.owner,
                                pre: b.pre,
                                post: b.post
                            }));
                            const senders: BalanceItem[] = deltas.filter((b) => b.post < b.pre);
                            const receivers: BalanceItem[] = deltas.filter((b) => b.post > b.pre);
                            // Process all sender-receiver pairs in parallel
                            const txPromises = [];
                            for (const sender of senders) {
                                for (const receiver of receivers) {
                                    const amount =
                                        senders.length > 1 &&
                                            receivers.length === 1 &&
                                            receiver.pre !== receiver.post
                                            ? Math.max(
                                                sender.pre - sender.post,
                                                receiver.post - receiver.pre
                                            )
                                            : Math.min(
                                                sender.pre - sender.post,
                                                receiver.post - receiver.pre
                                            );

                                    if (!(amount > 0)) continue;
                                    const txData: TxType = {
                                        txId: signature,
                                        fromAddress: sender.owner,
                                        toAddress: receiver.owner,
                                        token: checkToken,
                                        amount: amount,
                                        blockId: blockId,
                                        isNFTToken: false,
                                        status: trnxData.meta.err == null ? "success" : "failed",
                                        trnx_fee: 0,
                                        txType: null,
                                    };

                                    // Get both withdraw and deposit status in one Redis call
                                    const addresses = [txData.fromAddress, txData.toAddress].filter(Boolean);

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
                                        txData.trnx_fee = Number(await Utility_Helper.exponentialToDecimal(
                                            await Utility_Helper.bigNumberSafeMath(trnxData.meta.fee, "/", Math.pow(10, 9))
                                        ));
                                        console.log('Token txData >>>.', txData);

                                        txPromises.push(this.addTxToQueue(txData, this.deposit_withdraw_topic || ''));
                                    }

                                    sender.post += amount;
                                    receiver.pre += amount;
                                }
                            }

                            // Wait for all transactions to be queued
                            await Promise.all(txPromises);
                        }));
                    } else if (trnxData?.meta?.postBalances?.length > 0) {
                        /** for Sol Transactions */
                        const preBalances = trnxData.meta.preBalances;
                        const postBalances = trnxData.meta.postBalances;
                        // --- SOL Transfers ---
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
                                            trnx_fee: 0,
                                            txType: null
                                        };
                                        sender.change += amount;
                                        receiver.change -= amount;


                                        const addresses = [txData.fromAddress, txData.toAddress].filter(Boolean);

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
                                            txData.trnx_fee = Number(await Utility_Helper.exponentialToDecimal(
                                                await Utility_Helper.bigNumberSafeMath(trnxData.meta.fee, "/", Math.pow(10, 9))
                                            ));
                                            console.log('Coin txData >>>.', txData);
                                            await this.addTxToQueue(
                                                txData,
                                                this.deposit_withdraw_topic || ''
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

            }

        } catch (error) {
            console.error("Error processing block:", error);
            // Add longer delay on error before processing next block
            await this.sleep(5000);
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
                        maxSupportedTransactionVersion: 1
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
                });
        } catch (err: any) {
            console.log("🔥 ~ ~ getBlockTransactions error", err);
            return err.message;
        }
    };


    private sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

}
export const sol_tx_status_deposit_withdraw = new queue_transactions()
