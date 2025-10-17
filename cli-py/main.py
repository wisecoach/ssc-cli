from pyhmy import signing
from pyhmy import transaction
from eth_utils.curried import keccak

hash = keccak('crossCnt'.encode("utf-8"))

local_net = 'http://localhost:9500'
contract_addr = '0x660a4de91307f84b7de28057c25135d409015f2c'
tx = {
 'chainId': 2,
 'from': '0x15a128e599b74842BCcBa860311Efa92991bffb5',
 'gas': 5000000,
 'gasPrice': 3000000000,
 'nonce': 2,
 'shardID': 0,
 'to': contract_addr,
 'toShardID': 0,
 'crossShard': True,
 'value': 0,
 'data': '0x9ffeb78b'
}
tx_hash = transaction.send_raw_transaction(
    signing.sign_transaction(tx, '7472616374206b65e45ffeb29e933944f5027ef139f124f430641487e70ea9a1')
    .rawTransaction.hex(), local_net)
