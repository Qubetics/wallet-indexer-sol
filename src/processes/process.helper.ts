import database from "../config/db";
import { QueryTypes } from "sequelize";
import { CoinInterface, CoinsModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { BooleanEnum, CoinFamilyEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import { Op } from "sequelize";

import { PublicKey } from '@solana/web3.js';
import bs58 from "bs58";


class SolProcessHelper {
  public CoinsInterface: CoinInterface[];
  public WalletAddresses: WalletInterface[];

  constructor() {
    this.fetchSPLTokens();
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

  public async getMultipleKeyValuePairs(keyName: string, fields: string[]) {
    try {
      const upperFields = fields.map(f => f.toUpperCase());
      return new Promise((resolve, reject) => {
        redisClient.hmget(keyName, ...upperFields, (err, results) => {
          if (err) reject(err);
          else resolve(results);
        });
      });
    } catch (err: any) {
      console.error('getMultipleKeyValuePairs >>>>', err);
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

  public async setKeyValuePair(keyName: string, field: string, value: string) {
    try {
      redisClient.hset(keyName, field.toUpperCase(), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }

  public fetchSPLTokens = async () => {
    try {
      let tokens: any = await CoinsModel.findAll({
        attributes: ["coin_id", "coin_symbol", "coin_family", "is_token", "token_type", "decimals", "token_address", "for_swap", "added_by"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.SOL,
          [Op.or]: [{ token_type: 'SPL' }, { token_type: null }]
        },
        raw: true
      })
      for await (let token of tokens) {
        let findTokenData = await this.getKeyValuePair(
          `${config.TOKEN_TYPE_SOL}`,
          token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
        );
        if (!findTokenData) {
          this.setKeyValuePair(
            `${config.TOKEN_TYPE_SOL}`,
            token.token_address == null ? 'NativeCoin'.toUpperCase() : token.token_address.toUpperCase(),
            JSON.stringify(token)
          );
        }
      }
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
      let token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_SOL, address);
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('check_our_wallet_address >>>>>', err);
      return null;
    }
  };


  public check_our_wallet_address = async (address: string) => {
    try {
      let wallet_data: any = await this.getKeyValuePair(config.SOL_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
    } catch (error: any) {
      console.error('check_our_wallet_address >>>>>', error);
      return null;
    }
  };


  public getAllAccountKeys = (loadedAddresses: any, accountKeys: any) => {
    if (!loadedAddresses) return accountKeys;
    
    // Safely convert loaded readonly addresses with error handling
    const loadedReadOnly = (loadedAddresses.readonly || []).map((k: any) => {
        try {
            if (typeof k === 'string') {
                // Validate if it's a valid base58 PublicKey
                new PublicKey(k);
                return k;
            } else if (k && typeof k === 'object' && k.length === 32) {
                // If it's a Uint8Array or Buffer, convert to base58
                return new PublicKey(k).toBase58();
            } else {
                console.warn('💥 ~ Invalid loaded readonly key format, skipping:', k);
                return null;
            }
        } catch (keyError) {
            console.warn('💥 ~ Failed to convert loaded readonly key, skipping:', k, keyError);
            return null;
        }
    }).filter((k: any) => k !== null);
    
    // Safely convert loaded writable addresses with error handling
    const loadedWritable = (loadedAddresses.writable || []).map((k: any) => {
        try {
            if (typeof k === 'string') {
                // Validate if it's a valid base58 PublicKey
                new PublicKey(k);
                return k;
            } else if (k && typeof k === 'object' && k.length === 32) {
                // If it's a Uint8Array or Buffer, convert to base58
                return new PublicKey(k).toBase58();
            } else {
                console.warn('💥 ~ Invalid loaded writable key format, skipping:', k);
                return null;
            }
        } catch (keyError) {
            console.warn('💥 ~ Failed to convert loaded writable key, skipping:', k, keyError);
            return null;
        }
    }).filter((k: any) => k !== null);
    
    return [...accountKeys, ...loadedWritable, ...loadedReadOnly];
  }


  public buildTokenOwners = (meta: any, accountKeys: any) => {
    const map: any = {};
    const all = [...(meta.preTokenBalances || []), ...(meta.postTokenBalances || [])];
    for (const bal of all) {
        const account = accountKeys[bal.accountIndex]; // token account
        map[account] = {
            owner: bal.owner,
            mint: bal.mint,
        };
    }
    return map;
}


public decodeTokenInstruction = (ix: any, accountKeys: any) => {
  const data: any = Buffer.from(bs58.decode(ix.data));
  const discriminator = data[0];

  if (discriminator === 3) { // Transfer
      const amount = data.readBigUInt64LE(1);
      return {
          type: "transfer",
          source: accountKeys[ix.accounts[0]],
          destination: accountKeys[ix.accounts[1]],
          authority: accountKeys[ix.accounts[2]],
          amount: amount.toString(),
      };
  }

  if (discriminator === 12) { // TransferChecked
      const amount = data.readBigUInt64LE(1);
      return {
          type: "transferChecked",
          source: accountKeys[ix.accounts[0]],
          mint: accountKeys[ix.accounts[1]],
          destination: accountKeys[ix.accounts[2]],
          authority: accountKeys[ix.accounts[3]],
          amount: amount.toString(),
      };
  }

  return null;
}

 
}
const solProcessHelper = new SolProcessHelper();
export default solProcessHelper;