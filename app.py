#!/usr/bin/python3
# Copyright (c) BDist Development Team
# Distributed under the terms of the Modified BSD License.
import os
from logging.config import dictConfig

from flask import Flask, jsonify, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

RATELIMIT_STORAGE_URI = os.environ.get("RATELIMIT_STORAGE_URI", "memory://")

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri=RATELIMIT_STORAGE_URI,
)

# Use the DATABASE_URL environment variable if it exists, otherwise use the default.
# Use the format postgres://username:password@hostname/database_name to connect to the database.
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@postgres/aviacao")


pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={
        "autocommit": True,  # If True don’t start transactions automatically.
        "row_factory": namedtuple_row,
    },
    min_size=4,
    max_size=10,
    open=True,
    # check=ConnectionPool.check_connection,
    name="postgres_pool",
    timeout=5,
)


def is_decimal(s):
    """Returns True if string is a parseable float number."""
    try:
        float(s)
        return True
    except ValueError:
        return False


@app.route("/", methods=("GET",))
@app.route("/accounts", methods=("GET",))
@limiter.limit("1 per second")
def account_index():
    """Show all the accounts, most recent first."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            accounts = cur.execute(
                """
                SELECT account_number, branch_name, balance
                FROM account
                ORDER BY account_number DESC;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    return jsonify(accounts), 200


@app.route("/accounts/<account_number>/update", methods=("GET",))
@limiter.limit("1 per second")
def account_update_view(account_number):
    """Show the page to update the account balance."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            account = cur.execute(
                """
                SELECT account_number, branch_name, balance
                FROM account
                WHERE account_number = %(account_number)s;
                """,
                {"account_number": account_number},
            ).fetchone()
            log.debug(f"Found {cur.rowcount} rows.")

    # At the end of the `connection()` context, the transaction is committed
    # or rolled back, and the connection returned to the pool.

    if account is None:
        return jsonify({"message": "Account not found.", "status": "error"}), 404

    return jsonify(account), 200


@app.route(
    "/accounts/<account_number>/update",
    methods=(
        "PUT",
        "POST",
    ),
)
def account_update_save(account_number):
    """Update the account balance."""

    balance = request.args.get("balance")

    error = None

    if not balance:
        error = "Balance is required."
    if not is_decimal(balance):
        error = "Balance is required to be decimal."

    if error is not None:
        return jsonify({"message": error, "status": "error"}), 400
    else:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE account
                    SET balance = %(balance)s
                    WHERE account_number = %(account_number)s;
                    """,
                    {"account_number": account_number, "balance": balance},
                )
                # The result of this statement is persisted immediately by the database
                # because the connection is in autocommit mode.
                log.debug(f"Updated {cur.rowcount} rows.")

                if cur.rowcount == 0:
                    return (
                        jsonify({"message": "Account not found.", "status": "error"}),
                        404,
                    )

        # The connection is returned to the pool at the end of the `connection()` context but,
        # because it is not in a transaction state, no COMMIT is executed.

        return "", 204


@app.route(
    "/accounts/<account_number>/delete",
    methods=(
        "DELETE",
        "POST",
    ),
)
def account_delete(account_number):
    """Delete the account."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                with conn.transaction():
                    # BEGIN is executed, a transaction started
                    cur.execute(
                        """
                        DELETE FROM depositor
                        WHERE account_number = %(account_number)s;
                        """,
                        {"account_number": account_number},
                    )
                    cur.execute(
                        """
                        DELETE FROM account
                        WHERE account_number = %(account_number)s;
                        """,
                        {"account_number": account_number},
                    )
                    # These two operations run atomically in the same transaction
            except Exception as e:
                return jsonify({"message": str(e), "status": "error"}), 500
            else:
                # COMMIT is executed at the end of the block.
                # The connection is in idle state again.
                log.debug(f"Deleted {cur.rowcount} rows.")

                if cur.rowcount == 0:
                    return (
                        jsonify({"message": "Account not found.", "status": "error"}),
                        404,
                    )

    # The connection is returned to the pool at the end of the `connection()` context

    return "", 204


@app.route("/ping", methods=("GET",))
@limiter.exempt
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})


# Trocar para vir na string e nao no body

# Preço dos bilhetes pode ser fixo mas tem de ser mais caro os de 1 classe

# ver tips do slack

@app.route("/compra/<int:voo>/", methods=["POST"])
@limiter.limit("1 per second")
def compra_voo(voo):
    """Faz uma compra de bilhetes para um voo"""
    print("entered")
    with pool.connection() as conn:
        print("a")
        with conn.cursor() as cur:
            print("conexao iniciada")
            data = request.get_json()
            nif_cliente = data.get('nif')
            bilhetes = data.get('bilhetes')  # lista de tuplos (nome, classe)

            # Obter o balcao (aeroporto de partida) e o no_serie do voo
            cur.execute("""
                SELECT partida, no_serie
                FROM voo
                WHERE id = %(voo)s;
            """, {"voo": voo})

            resultado = cur.fetchone()
            if not resultado:
                return jsonify({"error": "Voo não encontrado."}), 404

            balcao, no_serie = resultado

            # Inserir na tabela venda
            cur.execute("""
                INSERT INTO venda (nif_cliente, balcao, hora)
                VALUES (%s, %s, NOW())
                RETURNING codigo_reserva;
            """, (nif_cliente, balcao))

            codigo_reserva = cur.fetchone()[0]

            # Inserir bilhetes
            for nome_passageiro, prim_classe in bilhetes:
                # Encontrar o primeiro assento disponível com a classe desejada
                cur.execute("""
                    SELECT lugar
                    FROM assento
                    WHERE no_serie = %s AND prim_classe = %s
                      AND lugar NOT IN (
                        SELECT lugar
                        FROM bilhete
                        WHERE voo_id = %s
                      )
                    ORDER BY lugar
                    LIMIT 1;
                """, (no_serie, prim_classe, voo))

                lugar_disponivel = cur.fetchone()
                if not lugar_disponivel:
                    return jsonify({
                        "error": f"Sem assentos {'de primeira classe' if prim_classe else 'económicos'} disponíveis para o voo."
                    }), 400

                lugar = lugar_disponivel[0]
                preco = 1200 if prim_classe else 400

                cur.execute("""
                    INSERT INTO bilhete (codigo_reserva, nome_passegeiro, prim_classe, voo_id, no_serie, lugar, preco)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (codigo_reserva, nome_passageiro, prim_classe, voo, no_serie, lugar, preco))

    return jsonify({
        "mensagem": "Compra realizada com sucesso.",
        "codigo_reserva": codigo_reserva,
        "bilhetes_comprados": bilhetes
    }), 201


if __name__ == "__main__":
    app.run()
