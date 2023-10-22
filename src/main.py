import itertools as it

from meza.io import read_csv, IterStringIO, write
from csv2ofx import utils
from csv2ofx.ofx import OFX
from csv2ofx.mappings.default import mapping as defaultMapping

from operator import itemgetter


def parse_date(r):
    raw: str = r["Date and time"]

    [time, date] = raw.split(None, 1)
    return date


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


mapping = {
    **defaultMapping,
    "currency": "AUD",
    "date": parse_date,
    "parse_fmt": "%d-%m-%y",
    "amount": get_amount,
    "type": get_type,
    "account": get_account,
    "description": itemgetter("Description"),
    "payee": itemgetter("Description"),
    "class": itemgetter("Category"),
    "id": get_id,
}

ofx = OFX(mapping)
records = read_csv("_data/ubank_data.csv", has_header=True)
groups = ofx.gen_groups(records)
trxns = ofx.gen_trxns(groups)
cleaned_trxns = ofx.clean_trxns(trxns)
data = utils.gen_data(cleaned_trxns)
content = it.chain([ofx.header(), ofx.gen_body(data), ofx.footer()])

with open("_out/ubank_data.ofx", "w") as f:
    write(f, IterStringIO(content))
