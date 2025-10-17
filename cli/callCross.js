const web3 = require("./init");
const global = require("./global")

const crossContract = new web3.eth.Contract(global.cross_abi, global.cross_addr)

async function getCount() {
    try {
        const ret = await crossContract.methods.getCount().call();
        console.log(ret)
    } catch (err) {
        console.error('Error calling read-only method:', err);
    }
}

async function crossCnt() {
    try {
        // const gas = await myContract.methods.incrementCounter().estimateGas({ from: web3.eth.defaultAccount });
        const tx = await crossContract.methods.crossCnt().send({
            from: web3.eth.defaultAccount,
            gas: 50000,
            gasPrice: web3.utils.toWei('20', 'gwei')
        });
        console.log('Transaction hash:', tx.transactionHash);
    } catch (err) {
        console.error('Error calling writable method:', err);
    }
}

// getCount().then()
crossCnt().then(res => {
    getCount().then()
})

