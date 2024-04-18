import itertools as it
from meza.io import read_csv, IterStringIO, write
from csv2ofx import utils
from csv2ofx.ofx import OFX
from csv2ofx.mappings.default import mapping as defaultMapping

from operator import itemgetter


from pathlib import Path
import click
from dateutil.parser import parse as parse_dttm

from datetime import datetime


def parse_date(r):
    raw: str = r["Date and time"]

    [time, date] = raw.split(None, 1)
    return date


def get_timespan_for_records(csv_by_acc: dict[str, list]):
    out: dict[str, datetime] = {}

    for acc in csv_by_acc:
        records = csv_by_acc[acc]

        for r in records:
            stripped_date = parse_date(r)
            parsed_date: datetime = parse_dttm(stripped_date, dayfirst=True)

            if "min" not in out:
                out["min"] = parsed_date

            if "max" not in out:
                out["max"] = parsed_date

            if parsed_date < out["min"]:
                out["min"] = parsed_date

            if parsed_date > out["max"]:
                out["max"] = parsed_date
    return out


def get_amount(r):
    amount: str = r["Debit"]

    if not amount:
        amount = r["Credit"]

    return amount.replace("$", "").replace(",", "")


def get_type(r):
    if r["Debit"]:
        return "DEBIT"
    return "CREDIT"


def is_internal(r) -> bool:
    if r["Payment type"] == "Internal Transfer":
        return True
    return False


def get_id(r):
    if is_internal(r):
        return r["Receipt number"]
    return r["Transaction ID"]


def get_account(r):
    to_acc = r["To account"]
    from_acc = r["From account"]

    if r["Credit"]:
        if to_acc:
            return to_acc

    return from_acc


def get_payee(r):
    if is_internal(r):
        if r["Credit"]:
            return r["From account"]
        return r["To account"]

    return r["Description"]


mapping = {
    **defaultMapping,
    "currency": "AUD",
    "date": parse_date,
    "parse_fmt": "%d-%m-%y",
    "amount": get_amount,
    "type": get_type,
    "account": get_account,
    "description": itemgetter("Description"),
    "payee": get_payee,
    "class": itemgetter("Category"),
    "id": get_id,
}


def read_csv_into_accounts(filename: str):
    records = read_csv(filename, has_header=True)

    out: dict[str, list] = {}

    for r in records:
        acc_name = get_account(r)

        if acc_name not in out:
            out[acc_name] = []

        out[acc_name] += [r]

    return out


def handle_records(records):
    ofx = OFX(mapping)

    groups = ofx.gen_groups(records)
    trxns = ofx.gen_trxns(groups)
    cleaned_trxns = ofx.clean_trxns(trxns)
    data = utils.gen_data(cleaned_trxns)
    content = it.chain([ofx.header(), ofx.gen_body(data), ofx.footer()])
    return content


@click.command()
@click.option(
    "-f",
    "--file-in",
    default="_data/ubank_data.csv",
    required=True,
    type=click.Path(),
    help="File to ingest",
)
@click.option(
    "-o",
    "--folder-out",
    default="_out",
    required=True,
    type=click.Path(),
    help="Folder to place output",
)
def read_data(file_in, folder_out):
    # file_in = "_data/ubank_data.csv"

    csv_by_account = read_csv_into_accounts(file_in)

    timespan = get_timespan_for_records(csv_by_account)

    time_format_out = "%Y-%m-%d"

    max_time = datetime.strftime(timespan["max"], time_format_out)
    min_time = datetime.strftime(timespan["min"], time_format_out)

    content_by_account = {}

    for acc in csv_by_account:
        content_by_account[acc] = handle_records(csv_by_account[acc])

    for acc in content_by_account:
        content = content_by_account[acc]

        out_path = Path(folder_out) / f"ubank_data_{acc}_{min_time}_{max_time}.ofx"

        # Throw error if path traversal somehow
        out_path.relative_to(folder_out)

        with open(out_path, "w") as f:
            write(f, IterStringIO(content))


if __name__ == "__main__":
    read_data()
