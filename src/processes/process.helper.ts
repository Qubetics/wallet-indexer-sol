import database from "../config/db";
import { QueryTypes } from "sequelize";
import { CoinInterface, CoinsModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { BooleanEnum, CoinFamilyEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import { Op } from "sequelize";

class SolProcessHelper {
  public CoinsInterface: CoinInterface[];
  public WalletAddresses: WalletInterface[];

  constructor() {
    this.fetchSPLTokens();
    // this.fetchERC721Tokens();
    this.fetchWalletAddress();
  }
  public async getKeyValuePair(keyName: string, field: string) {
    try {
      const getAsync = promisify(redisClient.HGET).bind(redisClient);
      const value = await getAsync(keyName, field.toUpperCase() || "");
      return value;
    } catch (err: any) {
      console.error('getKeyValuePair >>>>', err);
      return null;
    }
  }
  public updateLastBlockProcessed = async (lastBlockProcessed: number) => {
    try {
      const updatedBlockNumber = ++lastBlockProcessed;
      redisClient.set(
        config.REDISKEYS.LAST_BLOCK_NUMBER || '',
        updatedBlockNumber.toString()
      );
    } catch (error) {
      console.log(`💥 ~ ~ updateLastBlockProcessed error`, error);
    }
  };

  public getLastBlockProcessed = async (blockNumber: number) => {
    try {
      const getAsync = promisify(redisClient.get).bind(redisClient);
      console.log('config.REDISKEYS.LAST_BLOCK_NUMBER >>>', config.REDISKEYS.LAST_BLOCK_NUMBER);
      const value: any = await getAsync(config.REDISKEYS.LAST_BLOCK_NUMBER || '');
      console.log('value >>>', value);
      // if (value) {
      if (!isNaN(value) && value) { // Check if value is NOT NaN
        return parseInt(value);
      } else {
        console.log('value222 >>>', value);

        redisClient.set(
          config.REDISKEYS.LAST_BLOCK_NUMBER || '',
          blockNumber.toString()
        );
      }
    } catch (error) {
      console.log(`💥 ~ ~ getLastBlockProcessed error`, error);
    }
  };
  // public async setBlocks(field: string, key: string, minBlockConfirmation: number, newBlock: number | null) {
  //   try {
  //     let { getBlockNumber } = Blockchain_Helper.Web3.eth;
  //     let latestBlock = await getBlockNumber();
  //     latestBlock = latestBlock - minBlockConfirmation;
  //     if (newBlock) {
  //       if (newBlock < latestBlock) {
  //         await ethProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
  //       } else {
  //         console.log("No updation of blocks")
  //       }
  //     } else {
  //       await ethProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
  //     }
  //   } catch (err: any) {
  //     console.error("Error in  getBlocks", err)
  //     await global_helper.save_error_logs('ETH_setBlocks', err.message)
  //   }
  // }
  public async setKeyValuePair(keyName: string, field: string, value: string) {
    try {
      redisClient.hset(keyName, field.toUpperCase(), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }

  // public async getLastBlockProcessed(blockNumber: number) {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.LAST_BLOCK_NUMBER || "");
  //     if (value) return parseInt(value);
  //     redisClient.set(
  //       config.LAST_BLOCK_NUMBER || "",
  //       blockNumber.toString()
  //     );
  //   } catch (error: any) {
  //     console.error('getLastBlockProcessed >>>', error);
  //   }
  // }

  // public async updateLastBlockProcessed(lastBlockProcessed: number) {
  //   try {
  //     const updatedBlockNumber = ++lastBlockProcessed;
  //     redisClient.set(
  //       config.LAST_BLOCK_NUMBER || "",
  //       updatedBlockNumber.toString()
  //     );
  //   } catch (error: any) {
  //     console.error('updateLastBlockProcessed >>>>', error)
  //   }
  // }
  public async getHashTable(keyName: string) {
    try {
      const getAsync: any = promisify(redisClient.hvals).bind(redisClient)
      const value: any = await getAsync(keyName)
      return value;
    } catch (err: any) {
      console.error("Error in getHashTable>>>", err)
      return null;
    }
  }
  public async deleteKeyValuePair(keyName: string, field: string) {
    try {
      redisClient.hdel(keyName, field.toUpperCase(), function (err) {
        if (err) {
          throw err;
        }
        return true;
      })
    } catch (err: any) {
      console.error("Error in deleteKeyValuePair>>>", err)
      return false;
    }
  }



  public fetchSPLTokens = async () => {
    try {
      let tokens: any = await CoinsModel.findAll({
        attributes: ["coin_id", "coin_name", "coin_symbol", "coin_image", "coin_family", "coin_status", "is_token", "token_type", "decimals", "cmc_id", "is_on_cmc", "token_abi", "token_address", "for_swap", "added_by", "created_at", "updated_at"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.SOL,
          [Op.or]: [{ token_type: 'SPL' }, { token_type: null }]
        },
        raw: true
      })
      redisClient.set(config.TOKEN_TYPE_SOL || "", JSON.stringify(tokens));
    } catch (err: any) {
      console.error('fetchERC20Tokens >>>>>', err);
      await global_helper.save_error_logs('Sol_fetchSPLTokens', err.message)
    }
  }

  public fetchWalletAddress = async () => {
    try {
      const getaddressQuery = `SELECT ws.user_id,ws.wallet_address as address, ws.wallet_name FROM wallets ws FORCE INDEX (coin_family) JOIN coins cs ON ws.coin_id = cs.coin_id WHERE cs.coin_family = ${config.STATIC_COIN_FAMILY.SOL} GROUP BY ws.wallet_address`;
      const addresses: any = await database.db_write.query(
        getaddressQuery,
        { type: QueryTypes.SELECT }
      );
      for await (let wallet of addresses) {
        let findAddress = await this.getKeyValuePair(
          `${config.SOL_WALLET_ADDRESS}`,
          wallet.address.toUpperCase()
        );
        if (!findAddress) {
          this.setKeyValuePair(
            `${config.SOL_WALLET_ADDRESS}`,
            wallet.address.toUpperCase(),
            JSON.stringify(wallet)
          );
        }
      }
    } catch (error) {
      console.error('💥 ~ fetchWalletAddress error', error);
    }
  };

  public checkIfContract = async (address: string) => {
    try {
      const getAsync = promisify(redisClient.get).bind(redisClient);
      const value = await getAsync(config.TOKEN_TYPE_SOL || "");

      const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
      const token = erc20Tokens.find(
        (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address?.toUpperCase() === address?.toUpperCase()
      );
      if (token) return token;
      return null;
    } catch (error: any) {
      console.error('checkIfContract >>>', error);
      return null;
    }
  };

  // public fetchERC721Tokens = async () => {
  //   try {
  //     const getTokenQuery = `SELECT * FROM coins WHERE coin_status=1 AND coin_family=${config.STATIC_COIN_FAMILY.ETH} AND (token_type!='${config.TOKEN_TYPE_ETH}' OR token_type is null)`;

  //     const tokens: CoinInterface[] = await database.db_read.query(getTokenQuery, {
  //       type: Sequelize.QueryTypes.SELECT,
  //     });

  //     redisClient.set(config.TOKEN_TYPE721_ETH, JSON.stringify(tokens));
  //   } catch (error: any) {
  //     console.error('fetchERC721Tokens >>>>', error);
  //   }
  // };

  // public checkIfOurNFTContract = async (address: string) => {
  //   try {
  //     const getAsync = promisify(redisClient.get).bind(redisClient);
  //     const value = await getAsync(config.TOKEN_TYPE721_ETH);
  //     const erc721Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
  //     const token = erc721Tokens.find(
  //       (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address.toUpperCase() === address?.toUpperCase()
  //     );

  //     if (token) return token;
  //     return null;
  //   } catch (error: any) {
  //     console.error('checkIfOurNFTContract >>>>>', error);
  //     return null;
  //   }
  // };

  public check_our_wallet_address = async (address: string) => {
    try {
      let wallet_data: any = await this.getKeyValuePair(config.SOL_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
      // return address_data ? JSON.parse(address_data) : {};
    } catch (error: any) {
      console.error('check_our_wallet_address >>>>>', error);
      return null;
    }
  };
  public NativeCoinByCoinFamily = async (
    coin_family: CoinFamilyEnum
  ) => {
    try {
      const getAsync = promisify(redisClient.get).bind(redisClient);
      const value = await getAsync(config.TOKEN_TYPE_SOL || "");

      const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
      return erc20Tokens.find(
        (el: CoinInterface) => Number(el.is_token) === Number(BooleanEnum.false) && el.coin_family === coin_family
      );

    } catch (error: any) {
      console.error(`NativeCoinByCoinFamily >>>`, error);
      return 0;
    }
  }

  public CoinByCoinId = async (
    coin_id: number
  ) => {
    try {
      const getAsync = promisify(redisClient.get).bind(redisClient);
      const value = await getAsync(config.TOKEN_TYPE_SOL || "");

      const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
      return erc20Tokens.find(
        (el: CoinInterface) => el.coin_id === coin_id
      );

    } catch (error: any) {
      console.error(`CoinByCoinId >>>`, error);
      return null;
    }
  }

}
const solProcessHelper = new SolProcessHelper();
export default solProcessHelper;