import pandas as pd
import pyodbc


class Login:
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password

    def login_exec(self):
        print(self.email, self.password)

        connection_string = f"DRIVER=ODBC Driver 18 for SQL Server;SERVER=3.209.249.178;DATABASE=Treetech.ControleAcesso.Taesa;UID=SA;PWD=cJGcNWIYEt28zbX;TrustServerCertificate=Yes"
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        query = '''
                DECLARE @in_pswd nvarchar(150)
                DECLARE @in_user nvarchar(150)
                
                SET @in_user = ?
                SET @in_pswd = ?
                
                if EXISTS((SELECT Email FROM Usuario WHERE Email = @in_user))
                	BEGIN
                		if (@in_pswd = (SELECT TOP 1 Senha FROM Usuario WHERE LOWER(Email) = LOWER(@in_user)))
                			BEGIN
                				SELECT 'Usuário com login aprovado';
                			END
                		ELSE
                			BEGIN
                				SELECT 'Usuário com senha incorreta';
                			END
                	END
                ELSE
                	BEGIN
                		SELECT 'Usuário não encontrado';
                	END
            '''
        cursor.execute(query, self.email, self.password)

        columns = [column[0] for column in cursor.description]
        result = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return result[0][""]
