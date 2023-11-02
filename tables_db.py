from pydantic import BaseModel

class Aluno(BaseModel):
    nome : str
    email : str
    cpf : str
    endereco : str

class Professor(BaseModel):
    nome : str
    email : str
    cpf : str
    endereco : str


class Curso(BaseModel):
    descricao : str
    professor_id : int


class CursoAluno(BaseModel):
    idCurso : int
    idAluno : int