// SPDX-License-Identifier: MIT
pragma solidity >=0.7.0 <0.8.0;

contract TestContract {
    mapping(bytes32 => uint) public states;  // states 现在用 bytes32 作为键

    constructor() {
    }

    /**
     * 使用 bytes32[] 存储状态的 keccak256 哈希
     */
    function simulate(
        uint index,
        uint[] memory indexes,
        uint[] memory parentIndexes,
        uint[] memory shardIds,
        uint[] memory statesCounts,
        bytes32[] memory statesHashes  // 存储 keccak256(stateString)
    ) external returns (uint) {

        if (index >= indexes.length) {
            return index;
        }

        // 计算 statesHashes 的起始位置
        uint statesStart = 0;
        for (uint i = 0; i < index; i++) {
            statesStart += statesCounts[i];
        }

        // 更新状态
        for (uint i = 0; i < statesCounts[index]; i++) {
            states[statesHashes[statesStart + i]] = 1;
        }

        address[32] memory targetAddresses = [
            address(0x0279daca51618678cf262D4194D6D8EA28f292f3),
            address(0x0F7C618c515E7C1EAB161559fE0b4e435f74D5Ec),
            address(0x10C476219728b00B148d89e207d0577ae0903733),
            address(0x1fCfDeB2e52A84c63087b56A04EF1aAD3e2a0BdC),
            address(0x2556404acB5e4765c27cA6972b1FA2d27849B85e),
            address(0x2d0450bf211087F6Fe6E53CB4cFB44AeeCBB7201),
            address(0x305A7F4368303BaBb1942098B2B9D5E77Eb9E31e),
            address(0x3E7b6Fa055BC913aC75BE4a31F7a75b0cCFb5Cc1),
            address(0x4030A3D06cDB4237f8F3De615D1Ce41A5247f1cb),
            address(0x4DC4f854E1A9D7eba72Ed878C6704D9737f10000),
            address(0x50c37bDe03593f205De72AF2A553FbD900bAdbD4),
            address(0x5c8af83E27fA05824257528FdCc21f2FE033f7cC),
            address(0x64a0510Dc44C330a23447f2F51eC911D3b0b303d),
            address(0x6BD36cE30592Dc4C868b6BE6a1522085d723A3E3),
            address(0x706F1dC03d6867952FD4b956E578fDD7DDb325a1),
            address(0x79F73702e69135d9523df00A17C6C1DDeA4ff453),
            address(0x841B5d0e9B1fD1A76d2a827EE9754cDF5E1EB1A0),
            address(0x8A2a5135191C10F689116ce8240899551421020C),
            address(0x956CCc4c196Ac96Afe0e356A1d2604ab3866c31b),
            address(0x98ae2ca3d141C6D78b8770A2d3e211A20cFCDd5B),
            address(0xA10A66B26A1Ea7006C1D20BB4D94CcCdc53ca389),
            address(0xaB2Cb505d5264D0719D4071a3609198bf1824D0C),
            address(0xB3960D78A8cBCe86E34F86c19d02B89B042daeab),
            address(0xbA61260c06a9d538ae70d86b0E0fB6Ac7403A46e),
            address(0xc2Ee93E2FFda23cad0B7b4E56E7751363b3bF198),
            address(0xCf67222AaA7BbF6550EB229142c139f92e7C8D5c),
            address(0xd16889B17145aB44866e08B150499a4fCb527e9C),
            address(0xDA0a6cA37EcED61e2846e2ddb8A81EFEEEc01e69),
            address(0xe0FB3f563b27D99c13Ec5E6d167D6426eFb16bdE),
            address(0xeDD3c11eE1031e476F95b2cE42387e494bD9A038),
            address(0xf632CEAf97c2806728bb2B1Ec7A93bbAd8EE947e),
            address(0xf91A021dDAd289828b24Bd123774D2A943dB1cD2)
        ];

        // 递归调用子交易
        uint nextIndex = index + 1;
        while (nextIndex < indexes.length) {
            if (parentIndexes[nextIndex] == indexes[index]) {
                require(targetAddresses.length > 0, "Address array cannot be empty");
                require(shardIds[nextIndex] < targetAddresses.length, "Invalid shardId");
                address target = targetAddresses[shardIds[nextIndex]];
                nextIndex = TestContract(target).simulate(
                    nextIndex,
                    indexes,
                    parentIndexes,
                    shardIds,
                    statesCounts,
                    statesHashes
                );
            } else {
                break;
            }
        }

        // 清理状态
        for (uint i = 0; i < statesCounts[index]; i++) {
            delete states[statesHashes[statesStart + i]];
        }
        return nextIndex;
    }
}
