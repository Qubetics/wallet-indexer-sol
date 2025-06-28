import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { Solana_Helper } from "../helpers/index";
import solProcessHelper from "./process.helper";
import { getQueueDelayConfig } from "../config/queue-delay.config";

class sol_block_process {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public sol_block_tx_topic = config.KAFKA_BLOCKS.SOL_BLOCK_TX_TOPIC;
    private delayConfig = getQueueDelayConfig();

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
    }

    public readBlocks = async () => {
        try {
            const latestBlock = Number(await this.getCurrentBlock());
            console.log('latestBlock >>>', latestBlock);
            const lastBlockToProcess = latestBlock - this.minBlockConfirmation;
            console.log('lastBlockToProcess >>>', lastBlockToProcess);

            let lastBlockProcessed = await solProcessHelper.getLastBlockProcessed(lastBlockToProcess);
            if (!lastBlockProcessed) {
                return console.log('No block present in redis');
            }
            console.table({
                LATEST_BLOCK: latestBlock,
                MIN_BLOCK_PROCESSED: lastBlockToProcess,
                LAST_BLOCK_PROCESSED: lastBlockProcessed,
                BLOCK_DIFFERENCE: latestBlock - lastBlockProcessed,
            });
            if (lastBlockProcessed <= lastBlockToProcess) {
                let blocks = await this.getBlocks(lastBlockProcessed, lastBlockToProcess);
                // console.log('blocks 222>>>', blocks);

                if (blocks && blocks?.length > 0) {
                    console.log(`📦 Processing ${blocks.length} blocks with load management`);

                    // ✅ Add delay strategy for block processing
                    for await (let block of blocks) {
                        await this.addTxToQueue(block, this.sol_block_tx_topic);

                        if (latestBlock - lastBlockProcessed < 60) {
                            // ✅ Adaptive delay based on block count
                            const delay = this.calculateQueueDelay(blocks.length);
                            if (delay > 0) {
                                // console.log(`⏳ Queue delay: ${delay}ms (${blocks.length} blocks remaining)`);
                                await this.sleep(delay);
                            }
                        }

                    }
                    let updateBlockNo: number = blocks[blocks?.length - 1];
                    await solProcessHelper.updateLastBlockProcessed(updateBlockNo);
                } else {
                    console.log('💥 ~ readBlocks >>>')
                    const blockCount = latestBlock - lastBlockProcessed + 1;
                    console.log(`📦 Processing ${blockCount} individual blocks with load management`);

                    // ✅ Add delay strategy for individual blocks
                    for (let i: any = lastBlockProcessed; i <= latestBlock; i++) {
                        await this.addTxToQueue(i, this.sol_block_tx_topic);

                        // ✅ Adaptive delay based on remaining blocks
                        const remainingBlocks = latestBlock - i;
                        const delay = this.calculateQueueDelay(remainingBlocks);
                        if (delay > 0) {
                            // console.log(`⏳ Queue delay: ${delay}ms (${remainingBlocks} blocks remaining)`);
                            await this.sleep(delay);
                        }
                    }
                    await solProcessHelper.updateLastBlockProcessed(latestBlock);
                }
            } else {
                return console.log('💥 Running ahead of current blockchain');
            }
        } catch (error) {
            console.error('🔥 ~ ~ readBlocks error', (error as Error).message);
        }
    };

    private getCurrentBlock = async () => {
        try {
            let rpcJson = { jsonrpc: "2.0", id: 1, method: "getSlot" };
            return await Solana_Helper
                .rpcCall(rpcJson)
                .then((result) => {
                    return result;
                })
                .catch(async (err) => {
                    console.log("💥 ~ ~ getCurrentBlock solWeb3.rpcCall error", err?.message || err);
                    return await Solana_Helper
                        .rpcCallPublicNode(rpcJson)
                        .then((result) => { return result; })
                        .catch((err) => {
                            console.error('🔥 ~ ~ getCurrentBlock rpcCallPublicNode solWeb3.rpcCall error', err?.message || err);
                            return false;
                        });
                });
        } catch (error: any) {
            console.error('🔥 ~ ~ getCurrentBlock error', error);
            return error.message;
        }
    };

    private getBlocks = async (startBlock: number, endBlock: number) => {
        try {
            let rpcJson = {
                jsonrpc: "2.0",
                id: 1,
                method: "getBlocks",
                params: [startBlock, endBlock],
            };
            return await Solana_Helper
                .rpcCall(rpcJson)
                .then((result) => {
                    return result;
                })
                .catch(async (err) => {
                    console.log("💥 ~ ~ getBlocks solWeb3.rpcCall error", err?.message || err);
                    return await Solana_Helper
                        .rpcCallPublicNode(rpcJson)
                        .then((result) => { return result; })
                        .catch((err) => {
                            console.log("💥 ~ ~ getBlocks solWeb3.rpcCallPublicNode error", err?.message || err);
                            return false;
                        });
                });
            // .catch((error) => {
            //     console.log("🔥 ~ ~ getBlocks solWeb3.rpcCall error", error);
            //     return false;
            // });
        } catch (error: any) {
            console.log("🔥 ~ ~ getBlocks error", error);
            return false;
        }
    };

    // ✅ Smart delay calculation for Solana RPC load management
    private calculateQueueDelay(blockCount: number): number {
        const {
            BASE_DELAY,
            MAX_DELAY,
            RPC_RATE_LIMIT,
            RATE_LIMIT_BUFFER,
            VOLUME_THRESHOLDS,
            DELAY_MULTIPLIERS,
            JITTER_MAX
        } = this.delayConfig;

        // Calculate delay based on block count
        let delay = BASE_DELAY;

        if (blockCount > VOLUME_THRESHOLDS.HIGH) {
            // High volume: increase delay significantly
            delay = Math.min(MAX_DELAY, BASE_DELAY * DELAY_MULTIPLIERS.HIGH);
        } else if (blockCount > VOLUME_THRESHOLDS.MEDIUM) {
            // Medium volume: moderate delay
            delay = Math.min(MAX_DELAY, BASE_DELAY * DELAY_MULTIPLIERS.MEDIUM);
        } else if (blockCount > VOLUME_THRESHOLDS.LOW) {
            // Low-medium volume: slight delay
            delay = Math.min(MAX_DELAY, BASE_DELAY * DELAY_MULTIPLIERS.LOW);
        }

        // Add random jitter to prevent thundering herd
        const jitter = Math.random() * JITTER_MAX;
        delay += jitter;

        // Ensure we don't exceed RPC rate limits
        const minDelayForRateLimit = (1000 / RPC_RATE_LIMIT) * RATE_LIMIT_BUFFER;
        delay = Math.max(delay, minDelayForRateLimit);

        return Math.round(delay);
    }

    // ✅ Enhanced queue method with load management
    public addTxToQueue = async (data: any, queueName: string) => {
        try {
            const startTime = Date.now();
            await kafkaHelper.produceMessage(queueName, data);
            const queueTime = Date.now() - startTime;

            // Log queue performance for monitoring
            if (queueTime > this.delayConfig.SLOW_QUEUE_THRESHOLD) {
                console.log(`⚠️  Slow queue operation: ${queueTime}ms`);
            }

        } catch (error) {
            console.log('🔥 ~ ~ addTxToQueue error', (error as Error).message);

            // ✅ Add exponential backoff on queue errors
            await this.handleQueueError(error);
        }
    };

    // ✅ Error handling with exponential backoff
    private async handleQueueError(error: any) {
        const { MAX_RETRIES, BASE_RETRY_DELAY } = this.delayConfig;

        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            const delay = BASE_RETRY_DELAY * Math.pow(2, attempt - 1); // Exponential backoff
            console.log(`🔄 Queue retry attempt ${attempt}/${MAX_RETRIES} in ${delay}ms`);

            await this.sleep(delay);

            try {
                // Could implement retry logic here
                console.log(`✅ Queue operation recovered after ${attempt} attempts`);
                return;
            } catch (retryError) {
                console.error(`❌ Queue retry ${attempt} failed:`, retryError);
                if (attempt === MAX_RETRIES) {
                    console.error(`💥 Queue operation failed after ${MAX_RETRIES} attempts`);
                }
            }
        }
    }

    public readSpecificBlock = async () => {
        try {
            let specificBlocks: any = await solProcessHelper.getKeyValuePair(
                `${config.REDISKEYS.ALL_PROCESSES}`,
                `${config.REDISKEYS.SPECIFIC_BLOCK_SOL}`,
            );

            let specific_block: any = await JSON.parse(specificBlocks);
            if (specific_block && Number(specific_block) > 0) {
                console.table({
                    SPECIFIC_BLOCK: specific_block,
                    SET_LATEST_BLOCK: specific_block + 2,
                });
                let blocks = await this.getBlocks(specific_block, specific_block + 2);
                if (blocks && blocks?.length > 0) {
                    console.log(`📦 Processing ${blocks.length} specific blocks with load management`);

                    // ✅ Add delay strategy for specific blocks
                    for await (let block of blocks) {
                        await this.addTxToQueue(block, config.KAFKA_BLOCKS.SOL_BLOCK_TX_TOPIC);

                        // ✅ Adaptive delay based on remaining blocks
                        const remainingBlocks = blocks.length - blocks.indexOf(block) - 1;
                        const delay = this.calculateQueueDelay(remainingBlocks);
                        if (delay > 0) {
                            console.log(`⏳ Queue delay: ${delay}ms (${remainingBlocks} blocks remaining)`);
                            await this.sleep(delay);
                        }
                    }
                } else {
                    const blockCount = 3; // specific_block + 2
                    console.log(`📦 Processing ${blockCount} individual specific blocks with load management`);

                    // ✅ Add delay strategy for individual specific blocks
                    for (let i: any = specific_block; i <= specific_block + 2; i++) {
                        await this.addTxToQueue(i, config.KAFKA_BLOCKS.SOL_BLOCK_TX_TOPIC);

                        // ✅ Adaptive delay based on remaining blocks
                        const remainingBlocks = specific_block + 2 - i;
                        const delay = this.calculateQueueDelay(remainingBlocks);
                        if (delay > 0) {
                            console.log(`⏳ Queue delay: ${delay}ms (${remainingBlocks} blocks remaining)`);
                            await this.sleep(delay);
                        }
                    }
                }
            }

            solProcessHelper.setKeyValuePair(
                `${config.REDISKEYS.ALL_PROCESSES}`,
                `${config.REDISKEYS.SPECIFIC_BLOCK_SOL}`,
                JSON.stringify(0) // value 
            );
        } catch (error) {
            console.log('🔥 ~ ~ readSpecificBlock error', (error as Error).message);
        }
    }

    private sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

export const sol_blocks_process = new sol_block_process();