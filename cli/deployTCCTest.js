const {default: Wallet} = require("ethereumjs-wallet");
const {Web3} = require("web3");
const global = require("./global");

const privateKey = Buffer.from('7472616374206b65e45ffeb29e933944f5027ef139f124f430641487e70ea9a1', 'hex'); // 替换为你的私钥
const wallet = new Wallet(privateKey);
const address = wallet.getAddressString(); // 获取账户地址

deployEnv = "local"; // 部署环境
// deployEnv = "dev";

addresses = [
    "0x95a759428f9a8B4bc02E20086085F32B7A440463",       // 1
    "0x660A4dE91307f84b7dE28057C25135D409015F2C",       // 2
    "0xfC7D59Be0a1B36ef7D46059DCd896252c75E57c0",       // 3
    "0xA320e5eCfeBb88d0e6969E80EbBEF342320a392E",       // 2
    "0x3dd8F14e6d87a44d7a349197F5CB30a654e1320B",       // 3
    "0x6636dE6Ee2c432D7e07b1ac6b153E5E8327018a9",       // 2
    "0xA46F7A3465fc5CBe5aDe1F64F99862237953a4f1",       // 2
    "0x91f3B25CC7EE548eF5Fad8995482468f90dB041D",       // 1
    "0xa45a6e2395271bD2b92B00AEA6E305EBD22B95dF",       // 2
    "0x64FE3A70eFeD2D50728D140993555b7aBf0eDa5F",       // 2
    "0x2F0320c4E3203edbd8200E175acAB10043481858",       // 2
    "0x59c91871216E902d3c1131DC75D16a28A5681b95",       // 1
    "0xE55b817ac8B0f5D46C7c9558b6a7865258df7ED8",       // 2
    "0x9B79b26d84Ba6b61aff84E3EA4a5F5A397f15BbC",       // 1
    "0xDd66F429b278AD2076389520F4cF21e6c339d7d2",       // 0
]

shard_address = [
    "0xDd66F429b278AD2076389520F4cF21e6c339d7d2",       // 0
    "0x95a759428f9a8B4bc02E20086085F32B7A440463",       // 1
    "0x660A4dE91307f84b7dE28057C25135D409015F2C",       // 2
    "0xfC7D59Be0a1B36ef7D46059DCd896252c75E57c0",       // 3
]

shard_ip = [
    '10.7.95.200',
    '10.7.95.201',
    '10.7.95.202',
    '10.7.95.203',
]

numShard = 4
const abi = [
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": false,
                "internalType": "bytes32[]",
                "name": "keys",
                "type": "bytes32[]"
            },
            {
                "indexed": true,
                "internalType": "address",
                "name": "locker",
                "type": "address"
            }
        ],
        "name": "KeysLocked",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": false,
                "internalType": "bytes32[]",
                "name": "keys",
                "type": "bytes32[]"
            },
            {
                "indexed": true,
                "internalType": "address",
                "name": "unlocker",
                "type": "address"
            }
        ],
        "name": "KeysUnlocked",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": false,
                "internalType": "bytes32",
                "name": "key",
                "type": "bytes32"
            },
            {
                "indexed": false,
                "internalType": "string",
                "name": "message",
                "type": "string"
            }
        ],
        "name": "LockError",
        "type": "event"
    },
    {
        "inputs": [
            {
                "internalType": "bytes32[]",
                "name": "keys",
                "type": "bytes32[]"
            }
        ],
        "name": "areLocked",
        "outputs": [
            {
                "internalType": "bool[]",
                "name": "",
                "type": "bool[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {
                "internalType": "bytes32",
                "name": "key",
                "type": "bytes32"
            }
        ],
        "name": "isLocked",
        "outputs": [
            {
                "internalType": "bool",
                "name": "",
                "type": "bool"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {
                "internalType": "bytes32[]",
                "name": "keys",
                "type": "bytes32[]"
            }
        ],
        "name": "lock",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {
                "internalType": "bytes32[]",
                "name": "keys",
                "type": "bytes32[]"
            }
        ],
        "name": "unlock",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]
const bytecode = "608060405234801561001057600080fd5b50610605806100206000396000f3fe608060405234801561001057600080fd5b506004361061004c5760003560e01c806332a16f4e146100515780637dce953c14610095578063827d81b014610163578063cdf39929146101dc575b600080fd5b61007d6004803603602081101561006757600080fd5b8101908080359060200190929190505050610255565b60405180821515815260200191505060405180910390f35b61010c600480360360208110156100ab57600080fd5b81019080803590602001906401000000008111156100c857600080fd5b8201836020820111156100da57600080fd5b803590602001918460208302840111640100000000831117156100fc57600080fd5b909192939192939050505061027e565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b8381101561014f578082015181840152602081019050610134565b505050509050019250505060405180910390f35b6101da6004803603602081101561017957600080fd5b810190808035906020019064010000000081111561019657600080fd5b8201836020820111156101a857600080fd5b803590602001918460208302840111640100000000831117156101ca57600080fd5b9091929391929390505050610344565b005b610253600480360360208110156101f257600080fd5b810190808035906020019064010000000081111561020f57600080fd5b82018360208201111561022157600080fd5b8035906020019184602083028401116401000000008311171561024357600080fd5b9091929391929390505050610424565b005b600080600083815260200190815260200160002060009054906101000a900460ff169050919050565b606060008383905067ffffffffffffffff8111801561029c57600080fd5b506040519080825280602002602001820160405280156102cb5781602001602082028036833780820191505090505b50905060005b84849050811015610339576000808686848181106102eb57fe5b90506020020135815260200190815260200160002060009054906101000a900460ff1682828151811061031a57fe5b60200260200101901515908115158152505080806001019150506102d1565b508091505092915050565b60005b828290508110156103a257600083838381811061036057fe5b905060200201359050600080600083815260200190815260200160002060006101000a81548160ff021916908315150217905550508080600101915050610347565b503373ffffffffffffffffffffffffffffffffffffffff167fa7a49a74315ccd58b493c2d36224f0654c46d9d77087d425d583696eb3dd9da6838360405180806020018281038252848482818152602001925060200280828437600081840152601f19601f820116905080830192505050935050505060405180910390a25050565b60005b828290508110156104f357600083838381811061044057fe5b90506020020135905060008082815260200190815260200160002060009054906101000a900460ff16156104e5577f1882c205f4e06a580b7e8e6cc8fd6a6666a5c5caa8e6ddc0dc27fa60d718b05e816040518082815260200180602001828103825260128152602001807f4b657920616c7265616479206c6f636b656400000000000000000000000000008152506020019250505060405180910390a150506105cb565b508080600101915050610427565b5060005b8282905081101561054c57600160008085858581811061051357fe5b90506020020135815260200190815260200160002060006101000a81548160ff02191690831515021790555080806001019150506104f7565b503373ffffffffffffffffffffffffffffffffffffffff167ffdd7e7397b855a6a7b66352af58d110b19911530876c2b0a273ff6c3f0f5d61a838360405180806020018281038252848482818152602001925060200280828437600081840152601f19601f820116905080830192505050935050505060405180910390a25b505056fea2646970667358221220f993f3d2c59d6628d40db67f3f164d482f9b9e5282edabbcef062b7cffb9562364736f6c63430007060033"
const MAX_RETRIES = 100; // 最大重试次数
const RETRY_DELAY = 5000; // 重试延迟5秒

function get_endpoint(shardId) {
    if (deployEnv === "local") {
        return `http://localhost:${9500 + shardId * 40}`;
    }
    if (deployEnv === "dev") {
        return `http://${shard_ip[shardId]}:${9500 + shardId * 40}`;
    }
    return "http://127.0.0.1:9500"
}

/**
 * 带重试机制的合约部署
 * @param {object} web3 - Web3实例
 * @param {number} shardId - 分片ID
 * @param {number} deployIndex - 部署序号
 * @param {number} retryCount - 当前重试次数
 */
async function deployWithRetry(web3, shardId, deployIndex, retryCount = 0) {
    try {
        const SSCTestContract = new web3.eth.Contract(abi);
        const contract = await SSCTestContract.deploy({
            data: bytecode,
        }).send({
            from: web3.eth.defaultAccount,
            gas: 30000000,
            gasPrice: '20000010000'
        });
        console.log(`shard: ${shardId}, redeploy: ${deployIndex}, Contract Address: ${contract.options.address}`);
        return contract;
    } catch (error) {
        if (retryCount >= MAX_RETRIES) {
            console.error(`shard: ${shardId}, redeploy: ${deployIndex} failed after ${MAX_RETRIES} retries:`, error);
            throw error;
        }

        console.warn(`shard: ${shardId}, redeploy: ${deployIndex} failed, error ${error} (attempt ${retryCount + 1}), retrying in 5s...`);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
        return deployWithRetry(web3, shardId, deployIndex, retryCount + 1);
    }
}

/**
 * 串行部署单个分片内部的合约（15个合约按顺序部署）
 * @param {number} shardId - 分片ID
 */
async function deployContractsForShard(shardId) {
    const web3 = new Web3(get_endpoint(shardId));
    web3.eth.accounts.wallet.add('0x7472616374206b65e45ffeb29e933944f5027ef139f124f430641487e70ea9a1');
    web3.eth.defaultAccount = address; // 确保address已定义

    // 单个分片内部串行执行
    for (let j = 0; j < 15; j++) {
        await deployWithRetry(web3, shardId, j);
    }
}

/**
 * 并行部署所有分片（4个分片同时部署）
 */
async function deployAllShards() {
    const shardPromises = [];

    // 不同分片之间并行执行
    for (let i = 0; i < numShard; i++) {
        shardPromises.push(
            deployContractsForShard(i).catch(error => {
                console.error(`Shard ${i} deployment failed completely:`, error);
                return { shard: i, status: 'failed', error };
            })
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
deployAllShards();
