import sqlite3
import csv
import os
from argparse import ArgumentParser

def establish_db_con(file_in: str) -> sqlite3.Cursor:
    conn = sqlite3.connect(file_in)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    return cursor


def fetch_tables(cursor_in) -> list[dict] | None:
    cursor_in.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [dict(row) for row in cursor_in.fetchall()]


def execute_sql(sql_statement, cursor_in) -> list[dict] | None:
    cursor_in.execute(sql_statement)
    return [dict(row) for row in cursor_in.fetchall()]


def write_csv(data_in: list[dict], ref_file: str, table: str):
    if not os.path.exists(ref_file.replace('.', '')):
        os.mkdir(ref_file.replace('.', ''))

    with open(f'{ref_file.replace(".", "")}/{table}.csv', 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data_in[0].keys())
        writer.writeheader()
        writer.writerows(data_in)


def main(input_file):
    cursor = establish_db_con(input_file)
    tables = fetch_tables(cursor_in=cursor)
    for table in tables:

        try:
            table = table.get('name')
            data = execute_sql(f'SELECT * FROM {table}', cursor_in=cursor)
            write_csv(data_in=data, ref_file=input_file, table=table)

        except sqlite3.OperationalError as e:
            print(f'Operation error on table {table}: {e}')
            continue

        except Exception as e:
            print(f'Exception occurred on table {table}: {e}')
            continue


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument('-f', '--inputfile', help='Input file path, extension included')
    args = parser.parse_args()

    if not args.inputfile:
        file = input('Enter the input file: \n')
    else:
        file = args.inputfile

    main(file)


