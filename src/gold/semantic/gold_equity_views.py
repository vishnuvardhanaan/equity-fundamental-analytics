import logging
import sqlite3

from src.common.paths import GOLD_DB, GOLD_SQL_DIR


def company_views():
    # Paths to your SQLite database and SQL files
    db_path = GOLD_DB
    sql_files = [
        GOLD_SQL_DIR / "gold_equity_performance_views.sql",  # Main views
        GOLD_SQL_DIR / "gold_equity_explanation_views.sql",  # Explanation views
    ]

    def execute_sql_file(db_path, sql_file_path):
        logging.info("Processing Performance and Explanation Views")
        """Execute all statements in a SQL file on a SQLite database."""
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Load SQL file
            with open(sql_file_path, "r") as f:
                sql_script = f.read()

            # Split into individual statements
            statements = [
                stmt.strip() for stmt in sql_script.split(";") if stmt.strip()
            ]

            for stmt in statements:
                try:
                    cursor.execute(stmt)
                    logging.info(
                        f"Executed: {stmt.split()[0]}"
                    )  # First keyword: CREATE/DROP
                except sqlite3.Error as e:
                    logging.info(f"Error executing statement:\n{stmt}\nError: {e}")

            # Commit changes
            conn.commit()
            logging.info(f"SQL file executed successfully: {sql_file_path}\n")

    # Run all SQL files
    for sql_file in sql_files:
        execute_sql_file(db_path, sql_file)

    logging.info("Performance and Explanation Views Updated Successfully.")
