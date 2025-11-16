// SPDX-License-Identifier: MIT
pragma solidity >=0.7.0 <0.8.0;

struct CrossTx {
    bool exist;
    uint txId;
    mapping(bytes32 => bool) states;
    bytes32[] stateList;
}

contract TestContract {

    mapping(bytes32 => bool) public states;  // states 现在用 bytes32 作为键
    mapping(uint => CrossTx) private _crossTxs;

    event TxClosed(uint txId, bool commit);

    constructor() {
    }

    /**
     * 使用 bytes32[] 存储状态的 keccak256 哈希
     */
    function simulate(
        uint txId,
        uint index,
        uint maxDepthIndex,
        uint[] memory indexes,
        uint[] memory shardIds,
        uint[] memory statesCounts,
        bytes32[] memory statesHashes,
        uint[] memory subtreeSizes
    ) external payable {
        address[4] memory targetAddresses = [
            address(0x3dd8F14e6d87a44d7a349197F5CB30a654e1320B),
            address(0x660A4dE91307f84b7dE28057C25135D409015F2C),
            address(0x95a759428f9a8B4bc02E20086085F32B7A440463),
            address(0xfC7D59Be0a1B36ef7D46059DCd896252c75E57c0)
        ];

        if (index >= indexes.length) {
            return;
        }

        // 计算 statesHashes 的起始位置
        uint statesStart = 0;
        for (uint i = 0; i < index; i++) {
            statesStart += statesCounts[i];
        }

        CrossTx storage crossTx = _crossTxs[txId];
        if (!_crossTxs[txId].exist) {
            crossTx.exist = true;
            crossTx.txId = txId;
        }

        // 更新状态
        for (uint i = 0; i < statesCounts[index]; i++) {
            if (states[statesHashes[statesStart + i]] && !crossTx.states[statesHashes[statesStart + i]]) {
                for (uint j = 0; j < targetAddresses.length; j++) {
                    TestContract(targetAddresses[j]).rollback{value: 1200000000000000}(txId);
                }
                return;
            }
            if (!crossTx.states[statesHashes[statesStart + i]]) {
                states[statesHashes[statesStart + i]] = true;
                crossTx.states[statesHashes[statesStart + i]] = true;
                crossTx.stateList.push(statesHashes[statesStart + i]);
            }
        }

        if (index == maxDepthIndex) {
            for (uint i = 0; i < targetAddresses.length; i++) {
                TestContract(targetAddresses[i]).commit{value: 1300000000000000}(txId);
            }
        }

        uint next = index + 1;
        uint end = index + subtreeSizes[index];

        while (next < end) {
            TestContract(targetAddresses[shardIds[next]]).simulate{value: 1000000000000000}(txId, next, maxDepthIndex, indexes, shardIds, statesCounts, statesHashes, subtreeSizes);
            next += subtreeSizes[next];
        }
    }

    function commit(uint txId) external payable {
        CrossTx storage crossTx = _crossTxs[txId];
        for (uint i = 0; i < crossTx.stateList.length; i++) {
            states[crossTx.stateList[i]] = false;
        }
        delete _crossTxs[txId];
        emit TxClosed(txId, true);
    }

    function rollback(uint txId) external payable {
        CrossTx storage crossTx = _crossTxs[txId];
        for (uint i = 0; i < crossTx.stateList.length; i++) {
            states[crossTx.stateList[i]] = false;
        }
        delete _crossTxs[txId];
        emit TxClosed(txId, false);
    }
}
