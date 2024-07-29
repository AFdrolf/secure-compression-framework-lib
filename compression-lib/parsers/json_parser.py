import json


def parse_json(json_file_path):
    with open(json_file_path, 'rb') as json_file:
        json_data = json_file.read()
    return json_data
    print(type(json_data))
    for k in json_data.keys():
        print(k)


if __name__ == "__main__":
    json_file_path = "/Users/andresfg/Desktop/Cornell/Research/secure-processing-framework-project/secure-compression-framework-lib/compression-lib/json_example.json"
    print(parse_json(json_file_path))
