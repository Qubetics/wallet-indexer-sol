import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { Solana_Helper } from "../helpers/index";
import solProcessHelper from "./process.helper";
class sol_block_process {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public sol_block_tx_topic = config.KAFKA_BLOCKS.SOL_BLOCK_TX_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
    }

    public readBlocks = async () => {
        try {
            const latestBlock = Number(await this.getCurrentBlock());
            const lastBlockToProcess = latestBlock - this.minBlockConfirmation;
            let lastBlockProcessed = await solProcessHelper.getLastBlockProcessed(latestBlock);
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
                let blocks = await this.getBlocks(lastBlockProcessed, latestBlock);
                if (blocks && blocks?.length > 0) {
                    for await (let block of blocks) {
                        await this.addTxToQueue(block, this.sol_block_tx_topic);
                    }
                    let updateBlockNo: number = blocks[blocks?.length - 1];
                    await solProcessHelper.updateLastBlockProcessed(updateBlockNo);
                } else {
                    // return console.log(LogsConstants.BLOCK_LIST_EMPTY);
                    console.log('💥 ~ readBlocks >>>')
                    for (let i: any = lastBlockProcessed; i <= latestBlock; i++) {
                        await this.addTxToQueue(i, this.sol_block_tx_topic);
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
                .catch((err) => {
                    console.error('🔥 ~ ~ getCurrentBlock solWeb3.rpcCall error', err);
                    return false;
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
                .catch((error) => {
                    console.log("🔥 ~ ~ getBlocks solWeb3.rpcCall error", error);
                    return false;
                });
        } catch (error: any) {
            console.log("🔥 ~ ~ getBlocks error", error);
            return false;
        }
    };

    public addTxToQueue = async (data: any, queueName: string) => {
        try {
            await kafkaHelper.produceMessage(queueName, data);
        } catch (error) {
            console.log('🔥 ~ ~ addTxToQueue error', (error as Error).message);
        }
    };

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
                    for await (let block of blocks) {
                        await this.addTxToQueue(block, config.KAFKA_BLOCKS.SOL_BLOCK_TX_TOPIC);
                    }
                } else {
                    for (let i: any = specific_block; i <= specific_block + 2; i++) {
                        await this.addTxToQueue(i, config.KAFKA_BLOCKS.SOL_BLOCK_TX_TOPIC);
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
}

export const sol_blocks_process = new sol_block_process();