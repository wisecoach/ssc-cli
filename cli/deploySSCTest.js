const wallet = require("./init");
const {Web3} = require("web3");
require('dotenv').config();
const ssc_abi = require("../abi/ssc_abi.json")
const bytecodes = require("../abi/bytecodes.json")
const deployEnv = process.env.ENV; // 部署环境const
const shardNum = process.env.SHARD_NUM || 4
const MAX_RETRIES = 100; // 最大重试次数
const RETRY_DELAY = 5000; // 重试延迟5秒

const HEX_TO_BITS = {
    '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7,
    '8': 8, '9': 9, 'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15,
    'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 14, 'f': 15
};

function getShard(addr, n = 2) {
    const mask = (1 << n) - 1; // e.g., n=4 => 0b1111
    let bits = 0;
    let length = 0;

    // 跳过 "0x" 前缀（假设 addr 以 "0x" 开头）
    for (let i = 2; i < addr.length; i++) {
        const c = addr[i];
        if (!(c in HEX_TO_BITS)) {
            throw new Error(`Invalid hex character: ${c}`);
        }
        bits = (bits << 4) | HEX_TO_BITS[c];
        length += 4;

        if (length >= n) {
            break;
        }
    }

    const shift = Math.max(length - n, 0);
    const classNum = (bits >> shift) & mask;
    return classNum;
}

function get_endpoint(shardId) {
    if (deployEnv === "local") {
        return `http://localhost:${9500 + shardId * 40}`;
    }
    if (deployEnv === "dev") {
        return `http://10.7.95.${shardId+200}:${9500 + shardId * 40}`;
    }
    return "http://127.0.0.1:9500"
}

/**
 * 带重试机制的合约部署
 */
async function deployWithRetry(index, web3, account, abi, bytecode, shardId, deployIndex, retryCount = 0) {
    try {
        const contract = new web3.eth.Contract(abi);
        const deployedContract = await contract.deploy({
            data: bytecode,
        }).send({
            from: account,
            gas: 30000000,
            gasPrice: '20000010000'
        });
        return deployedContract;
    } catch (error) {
        if (retryCount >= MAX_RETRIES) {
            throw error;
        }

        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
        return deployWithRetry(index, web3, account, abi, bytecode, shardId, deployIndex, retryCount + 1);
    }
}

/**
 * 串行部署单个分片内部的合约（15个合约按顺序部署）
 * @param {number} shardId - 分片ID
 */
async function deployContractsForShard(shardId) {
    const web3 = new Web3(get_endpoint(shardId));
    for (let i = 0; i < wallet.length; i++) {
        web3.eth.accounts.wallet.add(wallet[i].privateKey)
    }
    const bytecode = bytecodes["ssc"][shardNum]
    let shard2Contract = {}
    let time = 0
    let cnt = 0
Outer:
    while (true) {
        time++
        const tasks = []
        for (let j = 0; j < wallet.length; j++) {
            tasks.push(deployWithRetry(cnt++, web3, wallet[j].address, ssc_abi, bytecode, shardId, 0))
        }
        let contracts = await Promise.allSettled(tasks)
        for (let i = 0; i < contracts.length; i++) {
            let addr = contracts[i].value.options.address
            let shard = getShard(addr, Math.log2(shardNum))
            if (!(shard in shard2Contract)) {
                shard2Contract[shard] = addr
            }
            if (Object.keys(shard2Contract).length >= shardNum) {
                console.log("Deployed contracts covering all shards:", shard2Contract)
                break Outer
            }
        }
        console.log(`has deployed contracts for shards: ${Object.keys(shard2Contract).length}, times=${time}, shards=${Object.keys(shard2Contract)}`)
    }
    let addresses = []
    for (let i = 0; i < shardNum; i++) {
        addresses.push(shard2Contract[i])
    }
    console.log(`finished deployment, need ${time} deployments, addresses=${addresses}`)
}

/**
 * 并行部署所有分片
 */
async function deployAllShards() {
    const shardPromises = [];

    // 不同分片之间并行执行
    for (let i = 0; i < shardNum; i++) {
        shardPromises.push(
            deployContractsForShard(i)
        );
    }

    const results = await Promise.allSettled(shardPromises);

    // 检查部署结果
    const failedShards = results.filter(r => r.status === 'rejected');
    if (failedShards.length > 0) {
        console.error(`${failedShards.length} shards failed to deploy completely`);
    } else {
        console.log("All contracts deployed successfully.");
    }
}

// 启动部署
deployAllShards()