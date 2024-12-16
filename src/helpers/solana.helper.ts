import axios from "axios";
import { config } from "../config/index";
import { AccountLayout } from "@solana/spl-token";
import { PublicKey, Connection } from "@solana/web3.js"
import { BooleanEnum, TokenStandard } from "../enum";

class Solana {
	public connection: Connection;
	public connectionNew: Connection;
	constructor() {
		this.connection = new Connection(config.config.NODE.SOL_RPC_URL || '', 'confirmed');
		this.connectionNew = new Connection("https://solana-mainnet.g.alchemy.com/v2/REDACTED");
	}
	// public rpcCall = async (body: {}) => {
	// 	try {
	// 		const result = async (resolve: any, reject: any) => {
	// 			const options = { headers: { "Content-Type": "application/json" } };
	// 			await axios.post(`${config.RPC_URL}`, body, options)
	// 				.then((data: any) => {
	// 					if (data.status == 200) {
	// 						resolve(data.data.result);
	// 					} else {
	// 						reject(false);
	// 					}
	// 				});
	// 		};
	// 		return new Promise(result);
	// 	} catch (error: any) {
	// 		console.log('🔥 ~ rpcCall web3.helper.ts error', error);
	// 		return error.message;
	// 	}
	// };


	public rpcCall = async (body: {}) => {
		try {
			const options = { headers: { "Content-Type": "application/json" } };
			const response = await axios.post(`${config.config.NODE.SOL_RPC_URL}`, body, options);

			if (response.status === 200) {
				return response.data.result;
			} else {
				console.log('🔥 ~ rpcCall response status error');
				throw new Error('Non-200 response status');
			}
		} catch (error: any) {
			console.log('🔥 ~ rpcCall web3.helper.ts error', error);

			// Check if the error is an AxiosError and handle accordingly
			if (axios.isAxiosError(error)) {
				if (error.response) {
					// Server responded with a status other than 200 range
					return `Error: ${error.response.status} - ${error.response.statusText}`;
				} else if (error.request) {
					// Request was made but no response was received
					return 'Error: No response received from server';
				} else {
					// Something happened in setting up the request
					return `Error: ${error.message}`;
				}
			} else {
				// Handle other types of errors
				return `Error: ${error.message}`;
			}
		}
	};

	public async getSolanaBalance(wallet: string, inSolana: boolean): Promise<string | number> {
		try {
			let balanceInWei = await this.connection.getBalance(new PublicKey(wallet));
			if (inSolana == true) {
				return Number(balanceInWei / 1000000000);
			}
			return balanceInWei;
		} catch (err) {
			console.log('🔥 ~ ~ getUserSolanaBalance node.solana.helper.ts error', err);
			return 0;
		}
	}

	// public async getUserCRC20TokenBalance(tokenAddress: string | null | undefined, walletAddress: string): Promise<number> {
	//     try {
	//         let tokenBalance = await solTokenObj.tokenBalance(walletAddress, tokenAddress);
	//         return tokenBalance;
	//     } catch (error: any) {
	//         console.log('🔥 ~ ~ getUserCRC20TokenBalance node.solana.helper.ts error', error);
	//         return 0;
	//     }
	// }

	public async getCRC20TokenBalance(tokenAddress: string, walletAddress: string): Promise<number> {
		try {
			const walletPublicKey = new PublicKey(walletAddress);
			const mintPublicKey = new PublicKey(tokenAddress);
			const tokenAccounts = await this.connectionNew.getTokenAccountsByOwner(walletPublicKey, {
				mint: mintPublicKey,
			});
			if (tokenAccounts.value?.length > 0) {
				const tokenAccount = tokenAccounts.value[0].account;
				const tokenAccountInfo = AccountLayout.decode(tokenAccount.data);
				const amount = tokenAccountInfo.amount;
				const mintInfo: any = await this.connectionNew.getParsedAccountInfo(mintPublicKey);
				const decimals = mintInfo.value.data.parsed.info.decimals;
				const balance = Number(amount) / (10 ** decimals);
				return balance;
			} else {
				return 0;
			}
		} catch (error: any) {
			console.log('🔥 ~ ~ getUserCRC20TokenBalance node.solana.helper.ts error', error);
			return 0;
		}
	}

	getBalance = async (
		address: string,
		coin: any
	) => {
		try {
			let balance: number = 0;
			if (coin.is_token == BooleanEnum.true) {
				switch (coin.token_type.toLowerCase()) {

					case TokenStandard.SPL:
						balance = await this.getCRC20TokenBalance(
							address, //// wallet_address
							coin.token_address ? coin.token_address : null/// contract_address
						)
						break;
					// case TokenStandard.ERC721:
					//     balance = await this.ERC721_Token_Fetch_Balance(
					//         address, //// wallet_address
					//         coin.token_address ? coin.token_address : null /// contract_address
					//     )
					//     break;
					default:
						balance = 0
						break;
				}
			} else {
				balance = Number(await this.getSolanaBalance(address, true));
			}
			return balance
		} catch (error: any) {
			console.error(`solanaHelper SOL_Fetch_Balance error >>`, error);
			return null;
		}
	}
}

const solana = new Solana();
export let Solana_Helper = solana;
