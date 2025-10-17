const web3 = require("./init");
const global = require("./global")
const {BN} = require('bn');


const myContract = new web3.eth.Contract(global.abi, global.contract_addr)

async function getCount() {
    try {
        const ret = await myContract.methods.getCount().call();
        console.log(ret)
    } catch (err) {
        console.error('Error calling read-only method:', err);
    }
}

async function incrementCount() {
    try {
        // const gas = await myContract.methods.incrementCounter().estimateGas({ from: web3.eth.defaultAccount });
        const tx = await myContract.methods.incrementCounter().send({
            from: web3.eth.defaultAccount,
            gas: 50000,
            gasPrice: web3.utils.toWei('10', 'gwei'),
        });
        console.log('Transaction hash:', tx.transactionHash);

    } catch (err) {
        console.error('Error calling writable method:', err);
    }
}

async function addCounter() {
    try {
        const number = 0x63726f73732d7368617264001111111110000000000000000000000000000000
        console.log(number)
        const tx = await myContract.methods.addCounter(number).send({
            from: web3.eth.defaultAccount,
            gas: 50000,
            gasPrice: web3.utils.toWei('10', 'gwei'),
        });
        console.log('Transaction hash:', tx.transactionHash);

    } catch (err) {
        console.error('Error calling writable method:', err);
    }
}

// getCount().then()
// incrementCount().then(res => {
    // getCount().then()
// })
addCounter().then(res => {
//     getCount().then()
})
