const web3 = require("./init");
const global = require("./global")

// 智能合约的 ABI 和 Bytecode
// const bytecode = contractFile.evm.bytecode.object;
// const abi = contractFile.abi;
// cdf86ba4 -> 86744558
const bytecode = "608060405234801561001057600080fd5b506040516102113803806102118339818101604052602081101561003357600080fd5b8101908080519060200190929190505050806000806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505061017d806100946000396000f3fe60806040526004361061001e5760003560e01c80638674455814610023575b600080fd5b6100596004803603604081101561003957600080fd5b81019080803590602001909291908035906020019092919050505061005b565b005b8082136101435760008260801b90506000816f63726f73732d7368617264000000000060801b18905060008054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166386744558600234816100ca57fe5b048360018801876040518563ffffffff1660e01b815260040180846fffffffffffffffffffffffffffffffff1916815260200183815260200182815260200193505050506000604051808303818588803b15801561012757600080fd5b505af115801561013b573d6000803e3d6000fd5b505050505050505b505056fea2646970667358221220b183fb28bcf464969d475d7fd336bb65c636f4cff1d6c6e00f4f41e5d44c60ba64736f6c63430007060033"
const abi = global.abi

const MyContract = new web3.eth.Contract(abi); // 使用ABI创建合约对象
const deployTx = MyContract.deploy({
    data: bytecode, // 合约的字节码
    arguments: [/* 构造函数的参数，如果有的话 */] // 如果你的合约有构造函数参数，请在这里提供它们
}).send({
    from: web3.eth.defaultAccount, // 发送者账户地址
    gas: 30000000, // 足够的gas来部署合约，这个值可能需要根据你的合约来调整
    gasPrice: '20000010000' // gas价格，以wei为单位，这个值也可能需要根据网络情况来调整
});
deployTx.on('transactionHash', (hash) => {
    console.log(`Transaction Hash: ${hash}`);
});
deployTx.on('receipt', (receipt) => {
    console.log(`Contract Address: ${receipt.contractAddress}`); // 合约被部署后的地址会在这里打印出来
    let cross_bytecode = "608060405234801561001057600080fd5b506040516102113803806102118339818101604052602081101561003357600080fd5b8101908080519060200190929190505050806000806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505061017d806100946000396000f3fe60806040526004361061001e5760003560e01c80638674455814610023575b600080fd5b6100596004803603604081101561003957600080fd5b81019080803590602001909291908035906020019092919050505061005b565b005b8082136101435760008260801b90506000816f63726f73732d7368617264000000000060801b18905060008054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166386744558600234816100ca57fe5b048360018801876040518563ffffffff1660e01b815260040180846fffffffffffffffffffffffffffffffff1916815260200183815260200182815260200193505050506000604051808303818588803b15801561012757600080fd5b505af115801561013b573d6000803e3d6000fd5b505050505050505b505056fea2646970667358221220b183fb28bcf464969d475d7fd336bb65c636f4cff1d6c6e00f4f41e5d44c60ba64736f6c63430007060033"
    const CrossCounter = new web3.eth.Contract(global.cross_abi); // 使用ABI创建合约对象
    const deployCrossTx = CrossCounter.deploy({
        data: cross_bytecode, // 合约的字节码
        arguments: [global.contract_addr] // 如果你的合约有构造函数参数，请在这里提供它们
    }).send({
        from: web3.eth.defaultAccount, // 发送者账户地址
        gas: 30000000, // 足够的gas来部署合约，这个值可能需要根据你的合约来调整
        gasPrice: '200000100000' // gas价格，以wei为单位，这个值也可能需要根据网络情况来调整
    });
    deployCrossTx.on('transactionHash', (hash) => {
        console.log(`Transaction Hash: ${hash}`);
    });
    deployCrossTx.on('receipt', (receipt) => {
        console.log(`Contract Address: ${receipt.contractAddress}`); // 合约被部署后的地址会在这里打印出来
    });
    deployCrossTx.on('error', (error) => {
        console.error(`Deployment failed: ${error}`);
    });
});
deployTx.on('error', (error) => {
    console.error(`Deployment failed: ${error}`);
});

async function sleep(time) {
    await new Promise(resoleve => setTimeout(resoleve, time));
}
