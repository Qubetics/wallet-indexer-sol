import { solWithdrawDepositProcess, solPendingWithdrawalProcess, solTxStatusUpdateProcess, sol_blocks_process, sol_tx_status_deposit_withdraw, sol_update_balance_controller } from "./processes/index.process"
import schedule from "node-schedule";
class ETHStartProcess {
  constructor() {
    this.read_block_process();
    this.readSpecificBlock();
    this.consume_read_block_queue();
    this.solWithdrawDepositProcess();
    this.getPendingWithdrawalTxProcess();
    this.updateTxStatusProcess();
    this.SolUpdateBalance();
    // this.testing_node()
  }
  public read_block_process() {
    schedule.scheduleJob("*/5 * * * * *", async function () {
      await sol_blocks_process.readBlocks();
    });
  }
  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await eth_blocks_process.testingNode();
  //   });
  // }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await sol_blocks_process.readSpecificBlock();
    })
  }
  public consume_read_block_queue() {
    setTimeout(async () => {
      await sol_tx_status_deposit_withdraw.startBlockTransactionQueue();
    }, 10000);
  }
  public async solWithdrawDepositProcess() {
    setTimeout(async () => {
      await solWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public getPendingWithdrawalTxProcess() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await solPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async updateTxStatusProcess() {
    setTimeout(async () => {
      await solTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async SolUpdateBalance() {
    setTimeout(async () => {
      await sol_update_balance_controller.update_wallet_balance();
    }, 10000);
  }
}

new ETHStartProcess();
