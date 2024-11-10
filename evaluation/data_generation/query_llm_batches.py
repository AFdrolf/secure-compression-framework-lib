import argparse
import json
from http.client import responses
from pathlib import Path

from openai import OpenAI
from openai.types import FileObject

CLIENT = OpenAI()

def upload_batch_file(batch_path: Path) -> FileObject:
    batch_input_file = CLIENT.files.create(
        file=open(batch_path, "rb"),
        purpose="batch"
    )
    return batch_input_file

def create_batch(batch_id: str) -> None:
    CLIENT.batches.create(
        input_file_id=batch_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={
          "description": "Conversation generation"
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-path", help="path to JSON file specifying batches to run", type=Path, required=False)
    parser.add_argument("--batch-id", help="ID of completed batch to download", type=str, required=False)
    args = parser.parse_args()

    if args.batch_path:
        upload = upload_batch_file(args.batch_path)
        create_batch(upload.id)
    else:
        current_batches = CLIENT.batches.list(limit=10)
        for b in current_batches:
            print(b)
            if args.batch_id:
                if b.status == "completed" and b.id == args.batch_id:
                    file_response = CLIENT.files.content(b.output_file_id)
                    for r in file_response.text.split("\n"):
                        if not r:
                            continue
                        parsed = json.loads(r)
                        content = parsed["response"]["body"]["choices"][0]["message"]["content"]
                        for conv in content.split("\n\n"):
                            split_conv = conv.split(" \\\n")
                            with (Path(__file__) / "llm.out").open("a") as p:
                                p.write("\\".join(split_conv) + "\n")
