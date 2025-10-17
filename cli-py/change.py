import os

file = "/mnt/E/gowork/src/github.com/harmony-one/ssc-harmony/test/configs/launch_config_local.txt"
validator_set = {
    0: [],
    1: [],
    2: [],
    3: [],
}
cnt = [0] * 4

addres = [
"one105clp5lulaqq96x64cvjt5fw2yh0he0frdlqdj",
"one10tkmez3662chu04cpww74m24gntfau0xxscnu3",
"one124t5xspw7gjnl5gs3d9063qeysnfuvp2jtv0j2",
"one12gywpr4z4w4xf68ys4n9t36lhpucwvwfv0qtcf",
"one12h20hukrqsqwplwj99jk8jg9mza0804g0asmax",
"one13c9xmt4f73t8ct73csrmnptz6ydd2t9fx08llk",
"one14gtzsk0kz6p5ne6uku4yy2q6438352fzhzf2ef",
"one14l04hctqxeyvcc2s2f9lcdry2j7sstaafjw9qc",
"one14mtgwx6w20h49kx478zmjxre52vn68x4k42dqz",
"one15axhtmd3qwcetjhd9svn2rmq006arg87h9juxv",
"one15lqeqw3pzx0j7ye7wajhd6pwcyrjlsj3u7wh6d",
"one16gqpjs5kn36ep7ave7xh8spepka4eclvfmj4ah",
"one17v5pd92zqyefurfw4kmqsctuep3f9rsr8zxcxm",
"one17z952d0e026a7p7hwz6aw3se6vjqh2q3c24flc",
"one18z8msmwpdyv785k53hhx7apq9k3tfd4e0tzngc",
"one1a08etj5n048fypmzl9srgu59fl8r6l52eek6z2",
"one1a22c8uxzgx5lflq32xtxtpv62jf0df9umtl0ml",
"one1a9haawcgnz82sz5cvczdqatlp86rd8mdmlr0gx",
"one1am8qejels680q6n69zfergeanmry8jah42jv06",
"one1ccru9g306yuaca09gj2n9rwly4mzqsq3v2a2yh",
"one1dlccw06cym5lsqm9d92eujgw5kdc8ffxjqgvg8",
"one1el7cgl3fmg6qza2zu5rjpvs6cyypsa5szhpzqf",
"one1he3ng5w9qgjrzdkenavqsw24wv5e7h5pe0xkk9",
"one1hgt3f20tuhjq7asjg8vtd2d9pdr3ef35wxpc6f",
"one1j6rw37xdly5lj9un6jumdlcszctu744unhyynu",
"one1jdk8lkmuevmz3q8x7snkg5wjku4qcnw928m0ah",
"one1jkgrzvv2qdtw2yz9hrw0hv0ceerhm82qry72dj",
"one1js0w47lydksjuycfz8p2yrwlxjntf3dh0yhcae",
"one1kyg82d7flnlexqmegnm9x69t89wxjnsc8l03n5",
"one1l79qj4f60ld5l4rn74rva7xl0r5k8extpyl5s7",
"one1lz489xqdpjy964d8yargj9s0vscdczua3twx82",
"one1mdw2c5vjwfl0utcyjrrnuv5kpamy7uzdfv5w4s",
"one1mpvhphe4g47n97q25nmse6a3r0mu6xax3ldm8u",
"one1mznftk88zyytdgjqpfgkfux6scu6wddaml6q5e",
"one1pkvyvdpk888z97pppqk5stdnh4y6w3kjeg0a9c",
"one1plw8wgmtjchad9c3v386aqtpf9hdfrsgt2x0jh",
"one1psjpp6u9hydd5vsc3evx47ch2h9ffzhjfq4pzg",
"one1q4rxtjn239g7r7qefe39kwz9quzshq4aj89k6g",
"one1qy8h25sfayfeyljwpm78kgn5vvhycz4nfp7t4z",
"one1r8l6elfzrulyu955wl2ehnnuqu2gl79kqmtvf0",
"one1s86mpvhp3lm3jy7len4xcxsl756p9kx256y8vu",
"one1savspenqaya3m8sa43p8m8pjqqhynpsavnxkcy",
"one1sfguxxxzc3fvual0cauczlq0lgw4jtfjthldpg",
"one1udj8m4e2vk9w4s8us954rpkwy8mh4zg4zktned",
"one1uqu43kecycvwgmzqrk7v3dr9l5wrtaet3py7jx",
"one1vrjgynvwmgxccyxlcry2v247n92xxkjm0edfv7",
"one1xstpjnr5wggzedgr5r58xvxxulrv0qyxfwn3hc",
"one1xvu8qtqkf6gsh55ap3qr2ezwr3ndnwks3dlwgc",
"one1y8qh5egvl0twg6grnp7dl953rqnhw28qetj7a4",
"one1yat3cy3kjfv0gs0g362jrv50e7d6pafm2m07qz",
]

addr2shard = {
"one105clp5lulaqq96x64cvjt5fw2yh0he0frdlqdj": 0,
"one12gywpr4z4w4xf68ys4n9t36lhpucwvwfv0qtcf": 0,
"one14mtgwx6w20h49kx478zmjxre52vn68x4k42dqz": 0,
"one15lqeqw3pzx0j7ye7wajhd6pwcyrjlsj3u7wh6d": 0,
"one1a22c8uxzgx5lflq32xtxtpv62jf0df9umtl0ml": 0,
"one1hgt3f20tuhjq7asjg8vtd2d9pdr3ef35wxpc6f": 0,
"one1l79qj4f60ld5l4rn74rva7xl0r5k8extpyl5s7": 0,
"one1mdw2c5vjwfl0utcyjrrnuv5kpamy7uzdfv5w4s": 0,
"one1r8l6elfzrulyu955wl2ehnnuqu2gl79kqmtvf0": 0,
"one1y8qh5egvl0twg6grnp7dl953rqnhw28qetj7a4": 0,
"one1yat3cy3kjfv0gs0g362jrv50e7d6pafm2m07qz": 0,
"one10tkmez3662chu04cpww74m24gntfau0xxscnu3": 1,
"one12h20hukrqsqwplwj99jk8jg9mza0804g0asmax": 1,
"one13c9xmt4f73t8ct73csrmnptz6ydd2t9fx08llk": 1,
"one1am8qejels680q6n69zfergeanmry8jah42jv06": 1,
"one1ccru9g306yuaca09gj2n9rwly4mzqsq3v2a2yh": 1,
"one1el7cgl3fmg6qza2zu5rjpvs6cyypsa5szhpzqf": 1,
"one1he3ng5w9qgjrzdkenavqsw24wv5e7h5pe0xkk9": 1,
"one1jdk8lkmuevmz3q8x7snkg5wjku4qcnw928m0ah": 1,
"one1jkgrzvv2qdtw2yz9hrw0hv0ceerhm82qry72dj": 1,
"one1kyg82d7flnlexqmegnm9x69t89wxjnsc8l03n5": 1,
"one1lz489xqdpjy964d8yargj9s0vscdczua3twx82": 1,
"one1mpvhphe4g47n97q25nmse6a3r0mu6xax3ldm8u": 1,
"one1pkvyvdpk888z97pppqk5stdnh4y6w3kjeg0a9c": 1,
"one1q4rxtjn239g7r7qefe39kwz9quzshq4aj89k6g": 1,
"one1udj8m4e2vk9w4s8us954rpkwy8mh4zg4zktned": 1,
"one1uqu43kecycvwgmzqrk7v3dr9l5wrtaet3py7jx": 1,
"one124t5xspw7gjnl5gs3d9063qeysnfuvp2jtv0j2": 2,
"one14gtzsk0kz6p5ne6uku4yy2q6438352fzhzf2ef": 2,
"one14l04hctqxeyvcc2s2f9lcdry2j7sstaafjw9qc": 2,
"one17z952d0e026a7p7hwz6aw3se6vjqh2q3c24flc": 2,
"one18z8msmwpdyv785k53hhx7apq9k3tfd4e0tzngc": 2,
"one1a08etj5n048fypmzl9srgu59fl8r6l52eek6z2": 2,
"one1dlccw06cym5lsqm9d92eujgw5kdc8ffxjqgvg8": 2,
"one1js0w47lydksjuycfz8p2yrwlxjntf3dh0yhcae": 2,
"one1s86mpvhp3lm3jy7len4xcxsl756p9kx256y8vu": 2,
"one1sfguxxxzc3fvual0cauczlq0lgw4jtfjthldpg": 2,
"one1vrjgynvwmgxccyxlcry2v247n92xxkjm0edfv7": 2,
"one1xstpjnr5wggzedgr5r58xvxxulrv0qyxfwn3hc": 2,
"one15axhtmd3qwcetjhd9svn2rmq006arg87h9juxv": 3,
"one16gqpjs5kn36ep7ave7xh8spepka4eclvfmj4ah": 3,
"one17v5pd92zqyefurfw4kmqsctuep3f9rsr8zxcxm": 3,
"one1a9haawcgnz82sz5cvczdqatlp86rd8mdmlr0gx": 3,
"one1j6rw37xdly5lj9un6jumdlcszctu744unhyynu": 3,
"one1mznftk88zyytdgjqpfgkfux6scu6wddaml6q5e": 3,
"one1plw8wgmtjchad9c3v386aqtpf9hdfrsgt2x0jh": 3,
"one1psjpp6u9hydd5vsc3evx47ch2h9ffzhjfq4pzg": 3,
"one1qy8h25sfayfeyljwpm78kgn5vvhycz4nfp7t4z": 3,
"one1savspenqaya3m8sa43p8m8pjqqhynpsavnxkcy": 3,
"one1xvu8qtqkf6gsh55ap3qr2ezwr3ndnwks3dlwgc": 3,
}


def form():
    with open(file, "r") as f:
        lines = f.readlines()
        for line in lines:
            args = line.strip().split(" ")
            address = args[0]
            bls_key = args[1]
            ip = args[2]

            shard_id = int("0x" + bls_key.split("/")[-1][0], 16) % 4
            port = 9000 + 40 * shard_id + 2 * cnt[shard_id]
            cnt[shard_id] += 1
            validator_set[shard_id].append({
                "address": address,
                "bls_key": bls_key,
                "shard_id": shard_id,
                "ip": ip,
                "port": port,
            })

    for shard_id, validators in validator_set.items():
        for validator in validators:
            print(f"{validator['address']} {validator['bls_key']} {validator['shard_id']} {validator['ip']} {validator['port']}")


def match():
    for addr in addres:
        print(addr2shard[addr])


match()