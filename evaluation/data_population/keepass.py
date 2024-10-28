import csv
import os
import subprocess
from pathlib import Path
from typing import Optional, TextIO

from evaluation.data_generation.keepass import KeepassCSVRow


def generate_keepass_xml(csv_file: Path, output_dir: Path) -> None:
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

    subprocess.run([kpcli_path, "db-create", "--set-key-file", keyfile_path, db_path])

    def kpcli_cmd(cmd_list: list[str], stdout_io: Optional[TextIO] = None) -> None:
        if not stdout_io:
            subprocess.run([kpcli_path, cmd_list[0], "--key-file", keyfile_path, "--no-password", *cmd_list[1:]])
        else:
            subprocess.run(
                [kpcli_path, cmd_list[0], "--key-file", keyfile_path, "--no-password", *cmd_list[1:]], stdout=stdout_io
            )

    columns = rows[0]
    groups = []
    for row in rows[1:]:
        entry = KeepassCSVRow(**{k: v for k, v in zip(columns, row)})
        if int(entry.version) > 0:
            kpcli_cmd(["edit", db_path, entry.account_name, "-g"])  # New password for account
            continue
        kpcli_cmd(["add", "-u", entry.username, "--url", entry.url, "-g", db_path, entry.account_name])
        if entry.group != "Root":
            if entry.group not in groups:
                kpcli_cmd(["mkdir", db_path, entry.group])
                groups.append(entry.group)
            kpcli_cmd(["mv", db_path, entry.account_name, entry.group])

    with (output_dir / f"{name}.xml").open("w") as f:
        kpcli_cmd(["export", db_path], stdout_io=f)
