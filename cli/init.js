const { Web3 } = require('web3');
const fs = require('fs');
const path = require('path');

const dirPath = path.resolve(__dirname, '../../ssc-harmony/.hmy/expr_accounts');

const web3 = new Web3();

try {
    const files = fs.readdirSync(dirPath); // 获取目录下所有文件/文件夹名

    files.forEach(file => {
        const filePath = path.join(dirPath, file);
        const stats = fs.statSync(filePath);

        if (stats.isFile()) {
            const key = fs.readFileSync(filePath, 'utf8');
            web3.eth.accounts.wallet.add("0x" + key.trim()); // 添加私钥到web3钱包
        } else if (stats.isDirectory()) {
        }
    });
} catch (err) {
    console.error('读取目录失败:', err);
}

console.log("init accounts, num=", web3.eth.accounts.wallet.length);

module.exports = web3.eth.accounts.wallet;