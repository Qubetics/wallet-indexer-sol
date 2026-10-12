import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { CoinInterface } from "../models";
import WalletModel from "../models/tables/model.wallets";
import { Solana_Helper } from "./solana.helper";
import { Utility_Helper } from "./utility.helper";

class WalletHelper {
    public sol_pending_balance_update_topic = config.config.KAFKA_BALANCE.SOL_PENDING_BALANCE_UPDATE_TOPIC;

    Fetch_Balance = async (
        address: string,
        coin: CoinInterface,
    ) => {
        try {
            let balance: number | null = null;
            if (coin) {
                switch (coin.coin_family) {
                    case CoinFamilyEnum.SOL:
                        balance = await Solana_Helper.getBalance(address, coin);
                        break;
                    default:
                        balance = null;
                        break;
                }
            }
            return balance !== null ? Number(await Utility_Helper.exponentialToDecimal(balance)) : null;
        } catch (error: any) {
            console.error(`WalletHelper Fetch_Balance error >>>`, error);
            return null;
        }
    }


    Update_Balance = async (
        wallet_address: string,
        coin: CoinInterface
    ) => {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            let balance: number | null = await this.Fetch_Balance(
                wallet_address,
                coin
            );
            console.log('balance >>>', balance);
            console.log('coin >>>', coin);
            if (balance !== null) {
                let wallet_data: any = await WalletModel.findOne({
                    attributes: ["balance"],
                    where: {
                        wallet_address: wallet_address,
                        coin_id: coin.coin_id,
                    }
                })
                if (wallet_data) {
                    await WalletModel.update({
                        balance: Number(balance),
                        status: 1

                    }, {
                        where: {
                            wallet_address: wallet_address,
                            coin_id: coin.coin_id,
                        }
                    });
                }
            }
            else {
                let data = {
                    coin_data: coin,
                    wallet_address: wallet_address,
                    queue_count: 0,
                }
                await kafkaHelper.produceMessage(this.sol_pending_balance_update_topic, data)
            }
            return balance;
        } catch (error: any) {
            console.error(`WalletHelper Update_Balance error >>>`, error);
            return null;
        }
    }


}
export let Wallet_Helper = new WalletHelper();
