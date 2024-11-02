import concurrent.futures
import csv
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, TextIO

from evaluation.data_generation.keepass import KeepassCSVRow


def generate_keepass_xml(csv_file: Path, output_dir: Path, cleanup: bool = True) -> None:
    """Given an input CSV file, generate a Keepass XML file for testing."""
    kpcli_path = os.getenv("KPCLIPATH")
    assert kpcli_path is not None, "KPCLIPATH envvar must be set"

    rows = []
    with csv_file.open() as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)

    name = csv_file.name.split(".")[0]
    keyfile_path = f"{output_dir}/{name}.key"
    db_path = f"{output_dir}/{name}.kdbx"

    subprocess.run([kpcli_path, "db-create", "--set-key-file", keyfile_path, db_path], stdout=subprocess.DEVNULL)

    def kpcli_cmd_args(
        cmd_list: list[str], bytes_in: Optional[bytes] = None, stdout_io: Optional[TextIO] = None
    ) -> dict:
        if not stdout_io:
            return {
                "args": [kpcli_path, cmd_list[0], "--key-file", keyfile_path, "--no-password", *cmd_list[1:]],
                "input": bytes_in,
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            }
        else:
            return {
                "args": [kpcli_path, cmd_list[0], "--key-file", keyfile_path, "--no-password", *cmd_list[1:]],
                "input": bytes_in,
                "stdout": stdout_io,
                "stderr": subprocess.DEVNULL,
            }

    columns = rows[0]
    groups = []
    kpcli_cmds = []
    for row in rows[1:]:
        entry = KeepassCSVRow(**{k: v for k, v in zip(columns, row)})
        if int(entry.version) > 0:
            kpcli_cmds.append(
                kpcli_cmd_args(["edit", db_path, entry.account_name, "-p"], bytes_in=entry.password.encode("utf-8"))
            )  # New password for account
            continue
        kpcli_cmds.append(
            kpcli_cmd_args(
                ["add", "-u", entry.username, "--url", entry.url, "-p", db_path, entry.account_name],
                bytes_in=entry.password.encode("utf-8"),
            )
        )
        if entry.group != "Root":
            if entry.group not in groups:
                kpcli_cmds.append(kpcli_cmd_args(["mkdir", db_path, entry.group]))
                groups.append(entry.group)
            kpcli_cmds.append(kpcli_cmd_args(["mv", db_path, entry.account_name, entry.group]))

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(subprocess.run, **cmd): cmd for cmd in kpcli_cmds}
        for future in concurrent.futures.as_completed(future_to_url):
            future.result()

    with (output_dir / f"{name}.xml").open("w") as f:
        subprocess.run(**kpcli_cmd_args(["export", db_path], stdout_io=f))

    if cleanup:
        Path(keyfile_path).unlink()
        Path(db_path).unlink()
