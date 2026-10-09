import axios from "axios";
import { config } from "../config/index";
import { AccountLayout } from "@solana/spl-token";
import { PublicKey, Connection } from "@solana/web3.js"
import { BooleanEnum, TokenStandard } from "../enum";

class Solana {
	public connection: Connection;
	public connectionNew: Connection;
	public connectionPublic: Connection;
	constructor() {
		this.connection = new Connection(config.config.NODE.SOL_RPC_URL || '', 'confirmed');
		this.connectionNew = new Connection(`https://solana-mainnet.g.alchemy.com/v2/${config.config.ALCHEMY_SOLANA_KEY}`);
		this.connectionPublic = new Connection("https://api.mainnet-beta.solana.com");
	}

	private async retryWithBackoff(fn: () => Promise<any>, maxRetries = 5, initialDelay = 1000) {
		let retries = 0;
		while (retries < maxRetries) {
			try {
				return await fn();
			} catch (error: any) {
				retries++;
				if (retries === maxRetries) {
					throw error;
				}

				// Check if it's a connection error that we should retry
				const shouldRetry = error.code === 'ECONNRESET' ||
					error.code === 'ETIMEDOUT' ||
					error.code === 'ECONNREFUSED' ||
					error.message?.includes('timeout');

				if (!shouldRetry) {
					throw error;
				}

				const delay = initialDelay * Math.pow(2, retries - 1);
				console.log(`RPC call failed, retrying in ${delay}ms... (Attempt ${retries}/${maxRetries})`);
				await new Promise(resolve => setTimeout(resolve, delay));
			}
		}
	}

	public rpcCall = async (body: {}) => {
		return this.retryWithBackoff(async () => {
			try {
				const options = {
					headers: { "Content-Type": "application/json" },
					timeout: 30000 // 30 second timeout
				};
				const response = await axios.post(`${config.config.NODE.SOL_RPC_URL}`, body, options);

				if (response.status === 200) {
					// JSON-RPC errors (e.g. -32015 unsupported tx version, -32009
					// slot skipped) come back as HTTP 200 with no `result` - throw
					// so callers see the real error and fall back to other nodes.
					if (response.data?.error) {
						throw new Error(`RPC error ${response.data.error.code}: ${response.data.error.message}`);
					}
					return response.data.result;
				} else {
					console.log('🔥 ~ rpcCall response status error');
					throw new Error('Non-200 response status');
				}
			} catch (error: any) {
				console.log('🔥 ~ rpcCall web3.helper.ts error', error?.message || error);

				// Check if the error is an AxiosError and handle accordingly
				if (axios.isAxiosError(error)) {
					if (error.response) {
						// Server responded with a status other than 200 range
						throw new Error(`Error: ${error.response.status} - ${error.response.statusText}`);
					} else if (error.request) {
						// Request was made but no response was received
						throw new Error('Error: No response received from server');
					} else {
						// Something happened in setting up the request
						throw new Error(`Error: ${error.message}`);
					}
				} else {
					// Handle other types of errors
					throw new Error(`Error: ${error.message}`);
				}
			}
		});
	};

	public rpcCallPublicNode = async (body: {}) => {
		for (const nodeUrl of config.config.SOL_PUBLIC_RPC_NODES) {
			try {
				const result = await this.retryWithBackoff(async () => {
					const options = {
						headers: { "Content-Type": "application/json" },
						timeout: 30000
					};
					const response = await axios.post(nodeUrl, body, options);
					if (response.status === 200) {
						// JSON-RPC errors (e.g. -32015 unsupported tx version, -32009
						// slot skipped) come back as HTTP 200 with no `result` - throw
						// so callers see the real error and fall back to other nodes.
						if (response.data?.error) {
							throw new Error(`RPC error ${response.data.error.code}: ${response.data.error.message}`);
						}
						return response.data.result;
					} else {
						console.log(`🔥 ~ rpcCall error: Non-200 status from ${nodeUrl}`);
						throw new Error(`Non-200 response status from ${nodeUrl}`);
					}
				});

				return result;

			} catch (error: any) {
				console.log(`⚠️ Node failed: ${nodeUrl} — ${error.message}`);
			}
		}

		throw new Error("All Solana RPC nodes failed.");
	};

	public async getSolanaBalance(wallet: string, inSolana: boolean): Promise<string | number | null> {
		try {
			let balanceInWei = await this.connection.getBalance(new PublicKey(wallet));
			if (balanceInWei === 0) {
				balanceInWei = await this.connectionNew.getBalance(new PublicKey(wallet));
			}
			if (balanceInWei === 0) {
				balanceInWei = await this.connectionPublic.getBalance(new PublicKey(wallet));
			}
			if (inSolana == true) {
				return Number(balanceInWei / 1000000000);
			}
			return balanceInWei;
		} catch (err) {
			console.log('🔥 ~ ~ getUserSolanaBalance node.solana.helper.ts error', err);
			return null;
		}
	}

	public async getCRC20TokenBalance(walletAddress: string, tokenAddress: string): Promise<number | null> {
		const fetchTokenAccountBalance = async (connection: any): Promise<number | null> => {
			const walletPublicKey = new PublicKey(walletAddress);
			const mintPublicKey = new PublicKey(tokenAddress);

			try {
				const tokenAccounts = await connection.getTokenAccountsByOwner(walletPublicKey, {
					mint: mintPublicKey,
				});

				if (tokenAccounts.value?.length > 0) {
					const tokenAccount = tokenAccounts.value[0].account;
					const tokenAccountInfo = AccountLayout.decode(tokenAccount.data);
					const amount = tokenAccountInfo.amount;
					const mintInfo: any = await connection.getParsedAccountInfo(mintPublicKey);
					const decimals = mintInfo.value.data.parsed.info.decimals;
					const balance = Number(amount) / (10 ** decimals);
					return balance;
				}
				return null;
			} catch (error) {
				console.log('🔥 ~ ~ Error fetching token balance:', error);
				return null;
			}
		};

		// Try fetching from the "new" connection
		let balance = await fetchTokenAccountBalance(this.connectionNew);

		// If no balance from the "new" connection, fall back to public node
		if (balance == null) {
			console.log('🚀 ~ Falling back to PUBLIC NODE');
			balance = await fetchTokenAccountBalance(this.connectionPublic);
		}

		return balance;
	}

	getBalance = async (
		address: string,
		coin: any
	) => {
		try {
			let balance: number | null = null;
			if (coin.is_token == BooleanEnum.true) {
				switch (coin.token_type) {

					case TokenStandard.SPL:
						balance = await this.getCRC20TokenBalance(
							address, //// wallet_address
							coin.token_address ? coin.token_address : null/// contract_address
						)
						break;
					default:
						balance = null
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
