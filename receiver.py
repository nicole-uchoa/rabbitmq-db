import pika
import psycopg2
# from fastapi import FastAPI, HTTPException
# from tables_db import Aluno, Curso, CursoAluno, Professor
from typing import List
from config import log_config
# import logging
import json
import sys
import os
from logging.config import dictConfig

dictConfig(log_config)


def main():
    # Configuração da conexão RabbitMQ
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    # Defina a fila que você deseja consumir
    queue = 'fila_api'
    channel.queue_declare(queue=queue)

    def callback(ch, method, properties, body):
        try:
            db_conn = connect_db()
            cursor = db_conn.cursor()
            metodo, query, data = get_data(body)
            # Executa query
            if metodo == "GET":
                try:
                    cursor.execute(query)
                    # Pega o retorno da query
                    dados = cursor.fetchall()
                    print("RESPOSTA: " + str(dados))
                except Exception as e:
                    print(f"ERRO no método GET \n Descrição erro: {e}")
                    cursor.close()
                    return
            else:
                try:
                    cursor.execute(query)
                    dados = cursor.rowcount
                    if dados == -1:
                        print("RESPOSTA: Dados de input inválidos")
                    else:
                        print("RESPOSTA: linhas alteradas: " + str(dados))

                except Exception as e:
                    print(f"ERRO no método {metodo} \n Descrição erro: {e}")
                    cursor.close()
                    return
            db_conn.commit()
            cursor.close()
            print(
                f" [x] Mensagem recebida e executada no banco de dados: {data}")
        except Exception as e:
            print(f"ERRO AO PROCESSAR MENSAGEM: {e}")

    channel.basic_consume(
        queue='fila_api', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def get_data(body):
    # pegar dados da mensagem
    data = body.decode('utf-8')
    data = json.loads(data)
    query = data.get("query")
    metodo = data.get('metodo')
    return metodo, query, data


def connect_db():
    # Configuração da conexão PostgreSQL
    db_conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )
    return db_conn


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
