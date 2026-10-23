export interface pending_trnx {
    tx_id: string;
    coin_id: number,
    is_token: boolean,
    token_type: string | null,
    token_address: string
}