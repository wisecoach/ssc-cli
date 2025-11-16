// SPDX-License-Identifier: GPL-3.0
pragma solidity >=0.7.0 <0.8.0;

contract LockContract {
    mapping(bytes32 => bool) private lockedKeys;
    
    // 事件用于记录操作
    event KeysLocked(bytes32[] keys, address indexed locker);
    event KeysUnlocked(bytes32[] keys, address indexed unlocker);
    event LockError(bytes32 key, string message);
    
    /**
     * @dev 对一组keys上锁
     * @param keys 要上锁的keys数组
     */
    function lock(bytes32[] calldata keys) external {
        for (uint256 i = 0; i < keys.length; i++) {
            bytes32 key = keys[i];
            
            // 检查key是否已经被锁定
            if (lockedKeys[key]) {
                // 如果已经锁定，触发错误事件并回滚交易
                emit LockError(key, "Key already locked");
                return;
            }
        }

        // 如果所有keys都未被锁定，则锁定它们
        for (uint256 i = 0; i < keys.length; i++) {
            lockedKeys[keys[i]] = true;
        }
        
        emit KeysLocked(keys, msg.sender);
    }
    
    /**
     * @dev 对一组keys解锁
     * @param keys 要解锁的keys数组
     */
    function unlock(bytes32[] calldata keys) external {
        for (uint256 i = 0; i < keys.length; i++) {
            bytes32 key = keys[i];
            
            // 解锁key（即使key未被锁定也可以执行，不会报错）
            lockedKeys[key] = false;
        }
        
        emit KeysUnlocked(keys, msg.sender);
    }
    
    /**
     * @dev 检查单个key是否被锁定
     * @param key 要检查的key
     * @return 如果key被锁定返回true，否则返回false
     */
    function isLocked(bytes32 key) external view returns (bool) {
        return lockedKeys[key];
    }
    
    /**
     * @dev 批量检查多个key是否被锁定
     * @param keys 要检查的keys数组
     * @return 返回每个key锁定状态的数组
     */
    function areLocked(bytes32[] calldata keys) external view returns (bool[] memory) {
        bool[] memory results = new bool[](keys.length);
        
        for (uint256 i = 0; i < keys.length; i++) {
            results[i] = lockedKeys[keys[i]];
        }
        
        return results;
    }
}