const { Web3 } = require('web3');
const Wallet = require('ethereumjs-wallet').default;


const privateKey = Buffer.from('7472616374206b65e45ffeb29e933944f5027ef139f124f430641487e70ea9a1', 'hex'); // 替换为你的私钥
const wallet = new Wallet(privateKey);
const address = wallet.getAddressString(); // 获取账户地址

const web3 = new Web3('http://localhost:9500');
web3.eth.accounts.wallet.add('0x7472616374206b65e45ffeb29e933944f5027ef139f124f430641487e70ea9a1'); // 添加私钥到web3钱包
web3.eth.defaultAccount = address; // 设置默认发送者账户

module.exports = web3;