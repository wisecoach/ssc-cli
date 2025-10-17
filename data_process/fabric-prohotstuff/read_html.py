from bs4 import BeautifulSoup
import os
import pandas as pd


def resolve_html(path):
    with open(path, "r") as f:
        soup = BeautifulSoup(f, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")
        headers = [header.text.strip() for header in rows[0].find_all("th")]
        data = []
        for row in rows[1:]:
            cols = row.find_all("td")
            cols = [col.text.strip() for col in cols]
            data.append(cols)
    return headers, data


def resolve_dir(path, output, old_path=None):
    files = os.listdir(path)
    html_files = [file for file in files if file.endswith(".html")]
    data = []
    for file in html_files:
        headers, rows = resolve_html(os.path.join(path, file))
        obj = {"file": file}
        tps = file.replace(".html", "").replace("tps=", "")
        obj["tps"] = float(tps)
        for i in range(len(headers)):
            obj[headers[i]] = rows[0][i]
        if old_path is not None:
            if not os.path.exists(os.path.join(old_path, file)):
                print("error: file {} not found".format(file))
            headers, rows = resolve_html(os.path.join(old_path, file))
            for i, header in enumerate(headers):
                if header == 'Avg Latency (s)':
                    obj[header] = rows[0][i]
        data.append(obj)
    df = pd.DataFrame(data)
    df = df.sort_values(by=["tps"])
    df.to_csv(output, index=False)
    return data


def remove_fail_data(path):
    files = os.listdir(path)
    html_files = [file for file in files if file.endswith(".html")]
    for file in html_files:
        headers, rows = resolve_html(os.path.join(path, file))
        obj = {"file": file}
        for i in range(len(headers)):
            obj[headers[i]] = rows[0][i]
        if int(obj["Fail"]) > 0:
            print(f"Remove {file}")
            os.remove(os.path.join(path, file))


# remove_fail_data("/mnt/E/blockchain/pro-hotstuff-test/caliper-benchmarks/test-prohotstuff")
resolve_dir("/mnt/E/blockchain/pro-hotstuff-test/caliper-benchmarks/test-prohotstuff", "test-prohotstuff.csv", old_path="/mnt/E/blockchain/pro-hotstuff-test/caliper-benchmarks/test-prohotstuff-old-1")
resolve_dir("/mnt/E/blockchain/pro-hotstuff-test/caliper-benchmarks/test-bft", "test-bft.csv")
