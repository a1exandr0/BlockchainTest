import sqlite3
import ast
from cfg import *
import pandas as pd
from email_module import send_email
from uuid import uuid4
import os


def initialize_postcode_data():
    df = pd.read_csv("all_us_zipcodes.csv", index_col="code")
    df.index = df.index.astype(str)
    return df.to_dict('index')


def initialize_all_cities():
    df = pd.read_csv("all_us_zipcodes.csv", index_col="code")
    return set(list(map(lambda x: x.lower(), df["city"].to_numpy())))


PO_DATA = initialize_postcode_data()
ALL_CITIES = initialize_all_cities()


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)

    return conn


def push_user(conn, user):
    sql = ''' INSERT INTO users(id, addressRaw, insertedAt, city, state, country, postCode)
              VALUES(?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, user)
    conn.commit()
    return cur.lastrowid


def create_tables(conn):
    sql = '''
        CREATE TABLE IF NOT EXISTS users (
            id string PRIMARY KEY,
            insertedAt timestamp NOT NULL,
            city text,
            state text,
            country text,
            postCode text,
            addressRaw text
        );
    '''
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def prepare_user(user_string):
    user_dict = ast.literal_eval(user_string)
    if "address" not in user_dict.keys():
        raise KeyError("Address not provided.")
    user_dict = {key: (user_dict[key] if key in user_dict.keys() and user_dict[key] is not None else "") for key in FIRST_ORDER_FIELDS}
    address_dict = ast.literal_eval(user_dict["address"])
    address_dict = {key: (address_dict[key] if key in address_dict.keys() and address_dict[key] is not None else "") for key in SECOND_ORDER_FIELDS}

    # combine original user json with filled address
    user = user_dict | clean_up_address(address_dict)

    return [user[key] for key in FIRST_ORDER_FIELDS + SECOND_ORDER_FIELDS]


def generate_report(conn):
    sql_by_states = "SELECT country, state, COUNT(distinct id) as users_amount FROM users WHERE state != '' group by 1, 2 order by 3 desc limit 10"
    sql_by_cities = "SELECT country, state, city, COUNT(distinct id) as users_amount FROM users WHERE state != '' and city != '' group by 1, 2, 3 order by 4 desc limit 10"
    _id = uuid4()
    df_by_states = pd.read_sql_query(sql_by_states, conn)
    df_by_cities = pd.read_sql_query(sql_by_cities, conn)
    df_by_states.to_csv(f"by_state{_id}.csv")
    df_by_cities.to_csv(f"by_city{_id}.csv")
    files = [f"by_state{_id}.csv", f"by_city{_id}.csv"]
    [send_email(files, mail) for mail in EMAIL_LIST]
    [os.remove(file) for file in files]


def fix_state(value):
    value = value.replace("us-", "")
    for state in STATES:
        if value == state["Code"].lower() or value == state["State"].lower():
            return state["Code"]
    return ""


def get_info_po_code(po_code):
    if po_code in PO_DATA.keys():
        return PO_DATA[po_code]
    else:
        return {}


def dict_clean(_dict):
    return dict((k, " ".join(v.lower().split()) if isinstance(v, str) else v) for k, v in _dict.items())


def clean_up_address(address_dict):
    address_dict = dict_clean(address_dict)
    if "postCode" in address_dict.keys():
        po_info = get_info_po_code(address_dict["postCode"])
        if not po_info:
            address_dict["postCode"] = ""
        if address_dict["city"] not in ALL_CITIES and po_info:
            address_dict["city"] = po_info["city"].lower()
        elif address_dict["city"] not in ALL_CITIES:
            address_dict["city"] = ""
        if "state" in address_dict.keys():
            address_dict["state"] = fix_state(address_dict["state"])
            if address_dict["state"] == "" and po_info:
                address_dict["state"] = po_info["state"]
    return address_dict
