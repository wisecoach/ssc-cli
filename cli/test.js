import Web3 from "web3";

const web3 = new Web3("https://api.s0.t.hmny.io/");
console.log(await web3.eth.getProtocolVersion());
console.log(await web3.eth.getChainId());