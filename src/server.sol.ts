import kafkaHelper from "./config/kafka.helper";
import {
  solWithdrawDepositProcess,
  solPendingWithdrawalProcess,
  solTxStatusUpdateProcess,
  sol_blocks_process,
  sol_tx_status_deposit_withdraw,
  sol_update_balance_controller,
} from "./processes/index.process";
import schedule from "node-schedule";
class ETHStartProcess {
  constructor() {
    this.read_block_process();
    this.readSpecificBlock();
    this.consume_read_block_queue();
    this.consume_read_block_queueB();
    this.solWithdrawDepositProcess();
    this.getPendingWithdrawalTxProcess();
    this.updateTxStatusProcess();
    this.solUpdateBalance();
    this.connectKafka();
  }
  public async connectKafka() {
    await kafkaHelper.connect();
  }
  public read_block_process() {
    schedule.scheduleJob("*/5 * * * * *", async function () {
      await sol_blocks_process.readBlocks();
    });
  }
  public readSpecificBlock() {
    schedule.scheduleJob("*/1 * * * *", async function () {
      await sol_blocks_process.readSpecificBlock();
    });
  }
  public consume_read_block_queue() {
    setTimeout(async () => {
      await sol_tx_status_deposit_withdraw.startBlockTransactionQueue();
    }, 10000);
  }
  public consume_read_block_queueB() {
    setTimeout(async () => {
      await sol_tx_status_deposit_withdraw.startBlockTransactionQueueB();
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
  public async solUpdateBalance() {
    setTimeout(async () => {
      await sol_update_balance_controller.updateWalletConsumer();
    }, 10000);
  }
}

new ETHStartProcess();
