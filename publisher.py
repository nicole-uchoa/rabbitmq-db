import pika
import psycopg2
from fastapi import FastAPI, HTTPException, Request, Body
from tables_db import Aluno, Curso, CursoAluno, Professor
from typing import List
from config import log_config
import logging
from logging.config import dictConfig
import json
dictConfig(log_config)
# Configurar conexão com o RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Definir a fila para enviar mensagens
queue_name = 'fila_api'
channel.queue_declare(queue=queue_name)

# Configuração API
app = FastAPI(debug=True)
logger = logging.getLogger('foo-logger')


@app.get("/")
def get_geral():
    return "Aqui não faz nada"

# CRUD aluno


@app.get("/aluno")
def get_aluno_all():
    mensagem = '{"metodo": "GET", "query": "SELECT * FROM aluno;"}'
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.get("/aluno/{nome}")
def get_aluno(nome: str):
    query = f"SELECT * FROM aluno where nome = '{nome}';"
    mensagem = {'metodo': 'GET', 'query': query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.post("/aluno")
def criar_aluno(request: Aluno):
    query = f"INSERT INTO public.aluno (nome, email, cpf, endereco) VALUES ('{request.nome}', '{request.email}', '{request.cpf}', '{request.endereco}');"
    body = {"nome": request.nome, "email": request.email,
            "cpf": request.cpf, "endereco": request.endereco}
    mensagem = {"metodo": "POST", "query": query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.put("/aluno/{id}")
def atualizar_aluno(id: int, request: Aluno):
    query = f"UPDATE public.aluno SET nome='{request.nome}', email='{request.email}', cpf='{request.cpf}', endereco='{request.endereco}' WHERE id={id};"
    body = {"nome": request.nome, "email": request.email,
            "cpf": request.cpf, "endereco": request.endereco}
    mensagem = {"metodo": "PUT", "query": query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.delete("/aluno/{id}")
def deletar_aluno(id: int):
    query = f"DELETE FROM public.aluno WHERE id={id};"
    mensagem = {"metodo": "DELETE", "query": query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"

# CRUD CURSO


@app.get("/curso")
def get_cursos_all():
    mensagem = '{"metodo": "GET", "query": "SELECT * FROM curso;"}'
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.get("/curso/{id}")
def get_curso(id: int):
    query = f"SELECT * FROM curso where id = {id};"
    mensagem = {'metodo': 'GET', 'query': query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.post("/curso")
def criar_curso(request: Curso):
    query = f"INSERT INTO public.curso (descricao, professor_id) VALUES ('{request.descricao}', '{request.professor_id}');"
    body = {"descricao": request.descricao,
            "professor_id": request.professor_id}
    mensagem = {"metodo": "POST", "query": query, "body": body}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.put("/curso/{id}")
def atualizar_aluno(id: int, request: Curso):
    query = f"UPDATE public.curso SET descricao='{request.descricao}', professor_id='{request.professor_id}' WHERE id={id};"
    body = {"descricao": request.descricao,
            "professor_id": request.professor_id}
    mensagem = {"metodo": "PUT", "query": query, "body": body}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.delete("/curso/{id}")
def deletar_aluno(id: int):
    query = f"DELETE FROM public.curso WHERE id={id};"
    mensagem = {"metodo": "DELETE", "query": query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"

# CRUD PROFESSOR


@app.get("/professor")
def get_aluno_all():
    mensagem = '{"metodo": "GET", "query": "SELECT * FROM professor;"}'
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.get("/professor/{id}")
def get_aluno(id: int):
    query = f"SELECT * FROM professor where id = {id};"
    mensagem = {'metodo': 'GET', 'query': query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.post("/professor")
def criar_aluno(request: Professor):
    query = f"INSERT INTO public.professor (nome, email, cpf, endereco) VALUES ('{request.nome}', '{request.email}', '{request.cpf}', '{request.endereco}');"
    body = {"nome": request.nome, "email": request.email,
            "cpf": request.cpf, "endereco": request.endereco}
    mensagem = {"metodo": "POST", "query": query, "body": body}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.put("/professor/{id}")
def atualizar_aluno(id: int, request: Professor):
    query = f"UPDATE public.professor SET nome='{request.nome}', email='{request.email}', cpf='{request.cpf}', endereco='{request.endereco}' WHERE id={id};"
    body = {"nome": request.nome, "email": request.email,
            "cpf": request.cpf, "endereco": request.endereco}
    mensagem = {"metodo": "PUT", "query": query, "body": body}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.delete("/professor/{id}")
def deletar_aluno(id: int):
    query = f"DELETE FROM public.professor WHERE id={id};"
    mensagem = {"metodo": "DELETE", "query": query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"

# CRUD CURSO-ALUNO


@app.get("/curso-aluno")
def get_cursos_all():
    mensagem = '{"metodo": "GET", "query": "SELECT * FROM cursoaluno;"}'
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.get("/curso-aluno/{id_curso}")
def get_curso(id_curso: int):
    query = f"SELECT * FROM cursoaluno where id_curso = {id_curso};"
    mensagem = {'metodo': 'GET', 'query': query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"Erro ao publicar mensagem: {e}"


@app.post("/curso-aluno")
def criar_curso(request: CursoAluno):
    query = f"INSERT INTO public.cursoaluno (id_curso, id_aluno) VALUES ('{request.id_curso}', '{request.id_aluno}');"
    body = {"id_curso": request.id_curso, "id_aluno": request.id_aluno}
    mensagem = {"metodo": "POST", "query": query, "body": body}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.put("/curso-aluno/{id_curso}/{id_aluno}")
def atualizar_aluno(id_curso: int, id_aluno: int, request: CursoAluno):
    query = f"UPDATE public.cursoaluno SET id_curso='{request.id_curso}', id_aluno='{request.id_aluno}' WHERE id_curso={id_curso} and id_aluno={id_aluno};"
    body = {"id_curso": request.id_curso, "id_aluno": request.id_aluno}
    mensagem = {"metodo": "PUT", "query": query, "body": body}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"


@app.delete("/curso-aluno/{id_curso}/{id_aluno}")
def deletar_aluno(id_curso: int, id_aluno: int):
    query = f"DELETE FROM public.cursoaluno WHERE id_curso={id_curso} and id_aluno={id_aluno};"
    mensagem = {"metodo": "DELETE", "query": query}
    mensagem = json.dumps(mensagem)
    try:
        channel.basic_publish(
            exchange='', routing_key=queue_name, body=mensagem)
        return "MENSAGEM ENVIADA"
    except Exception as e:
        return f"ERRO NO ENVIO DA MENSAGEM \n Erro: {e}"
