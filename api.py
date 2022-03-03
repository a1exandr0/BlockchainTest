from fastapi import FastAPI
from utils import create_connection, push_user, create_tables, prepare_user, generate_report
from sqlite3 import IntegrityError
from cfg import *

app = FastAPI()
db_conn = create_connection(CONNECTION_FILE)
create_tables(db_conn)


@app.post("/user")
async def insert_data(user_json: str):
    try:
        user = prepare_user(user_json)
        row_id = push_user(db_conn, user)
        if row_id % REPORT_INTERVAL == 0:
            generate_report(db_conn)
        return {"code": 200, "row_id": row_id}
    except IntegrityError:
        return {"code": 400, "row_id": None, "message": "User already exists in DB."}
    except KeyError:
        return {"code": 401, "row_id": None, "message": "Address not provided."}
