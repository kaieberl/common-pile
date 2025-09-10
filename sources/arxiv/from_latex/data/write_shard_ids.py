import xml.etree.ElementTree as ET

with open("arXiv_src_manifest.xml", "r") as rf:
    xml_data = rf.read()

root = ET.fromstring(xml_data)

# Extract filenames
filenames = [file.find("filename").text for file in root.findall("file")]

with open("arxiv-shards.txt", "w") as wf:
    for n in filenames:
        if "1501" <= n.split("_")[-2] <= "3000":
            wf.write(n[4:-4] + "\n")