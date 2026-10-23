import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { Wallet_Helper } from "../helpers";
import { CoinsModel, WalletModel } from "../models";
import solProcessHelper from "./process.helper";

class solUpdateBalanceController {
    public sol_pending_balance_update_group_id =
        config.config.KAFKA_BALANCE.SOL_PENDING_BALANCE_UPDATE_GROUP_ID;
    public sol_pending_balance_update_topic =
        config.config.KAFKA_BALANCE.SOL_PENDING_BALANCE_UPDATE_TOPIC;

    public updateWalletConsumer = async () => {
        const consumer = await kafkaHelper.kafka.consumer({
            groupId: this.sol_pending_balance_update_group_id,
        });
        await consumer.subscribe({
            topic: this.sol_pending_balance_update_topic,
            fromBeginning: true,
        });
        await consumer
            .run({
                eachMessage: async ({
                    topic,
                    partition,
                    message,
                }: {
                    topic: any;
                    partition: any;
                    message: any;
                }) => {
                    await this.updateBalance(JSON.parse(message.value.toString()));
                },
            })
            .catch((err: Error) => {
                console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
            });
    };

    public updateBalance = async (data: any) => {
        try {
            if (data.queue_count > 5) {
                console.log(`Balance update attempts exceeded (5) for address: ${data.wallet_address}`);
                return;
            }

            const balance: any = await Wallet_Helper.Fetch_Balance(
                data.wallet_address,
                data.coin_data
            );
            if (balance !== null) {
                const currentBalance = data.balance !== undefined ? Number(data.balance) : undefined;
                if (currentBalance === undefined || currentBalance !== balance) {
                    await WalletModel.update(
                        { balance: Number(balance) },
                        {
                            where: {
                                wallet_address: data.wallet_address,
                                coin_id: data.coin_data.coin_id,
                            },
                        }
                    );
                } else {
                }
            } else {
                if (data.queue_count <= 5) {
                    const queueData: any = {
                        coin_data: data.coin_data,
                        wallet_address: data.wallet_address,
                        queue_count: data.queue_count + 1,
                    }
                    if (data.balance !== undefined) {
                        queueData.balance = data.balance;
                    }
                    await kafkaHelper.produceMessage(
                        this.sol_pending_balance_update_topic,
                        queueData
                    );
                }
            }

        } catch (err: any) {
            console.error("Error in updateBalance", err);
        }
    };

    public updateWalletBalanceByWallet = async () => {
        try {
            let coin_limit_count = 0;
            const limit = 50;
            const findCoinCounter = await solProcessHelper.getKeyValuePair(
                config.config.REDISKEYS.ALL_PROCESSES,
                "WalletCount"
            );

            if (findCoinCounter) {
                coin_limit_count = Number(findCoinCounter);
            }
            const wallets: any = await WalletModel.findAll({
                attributes: ["wallet_address", "balance"],
                where: {
                    coin_family: CoinFamilyEnum.SOL,
                },
                include: [
                    {
                        attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
                        model: CoinsModel,
                        as: "coin_data",
                    },
                ],
                limit,
                offset: coin_limit_count,
                order: [['wallet_id', 'ASC']],
                nest: true,
            });
            if (!wallets) {
                await solProcessHelper.setKeyValuePair(
                    config.config.REDISKEYS.ALL_PROCESSES,
                    "WalletCount",
                    "0"
                );
            }
            if (wallets.length > 0) {
                for (const wallet of wallets) {
                    const coin_data = wallet.coin_data;
                    if (!coin_data) return false;
                    const payload = {
                        coin_data: coin_data,
                        wallet_address: wallet.wallet_address,
                        balance: wallet.balance,
                        queue_count: 0,
                    };
                    await kafkaHelper.produceMessage(
                        this.sol_pending_balance_update_topic,
                        payload
                    );
                }
            }
            if (wallets.length < limit) {
                await solProcessHelper.setKeyValuePair(
                    config.config.REDISKEYS.ALL_PROCESSES,
                    "WalletCount",
                    "0"
                );
            } else {
                coin_limit_count += limit;
                await solProcessHelper.setKeyValuePair(
                    config.config.REDISKEYS.ALL_PROCESSES,
                    "WalletCount",
                    coin_limit_count.toString()
                );
            }
            return true;
        } catch (err: any) {
            console.error("Error in updateWalletBalanceByWallet", err);
        }
    };
}

export const sol_update_balance_controller = new solUpdateBalanceController();
