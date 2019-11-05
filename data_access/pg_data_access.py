"""
Created on 10 oct. 2018

@author: egaudenti
"""

import psycopg2

from .pg_db_params import PgDbParams


class PgDataAccess(object):
    """
    Clase que permite el acceso a DB.
    """

    def __init__(self, p_db_params):
        """
        Constructor
        """
        self.params = p_db_params
        self.conn = psycopg2.connect(str(self.params))
        self.cursor = self.conn.cursor()

    def set_new_connection(self):
        """
        Reemplaza la conexion actual y el cursor.
        """
        self.close_conn()
        self.conn = psycopg2.connect(str(self.params))
        self.cursor = self.conn.cursor()

    def change_all_params(self, params):
        """
        Dado una entidad de parametros se vuelve a crear toda la conexion
        """
        if type(params) != PgDbParams:
            raise TypeError("El tipo de dato debe ser: DbParams")
        self.params = params
        self.set_new_connection()

    def get_cursor(self):
        """
        Retorna un nuevo cursor con la conexion actual
        """
        return self.conn.cursor()

    def change_db(self, new_db_name):
        """
        Permite cambiar de base en la misma conexion postgres
        """
        self.params.set_all(p_dbname=new_db_name)
        self.set_new_connection()

    def execute_query_with_return(self, query):
        """
        ejecuta una consulta y retorna el resultado
        """
        #    stk = inspect.stack()
        #   save_log_info(stk[0][1], "Exec :" + query, stk[0][3])
        if self.cursor.closed:
            self.set_new_connection()

        self.cursor.execute(query)
        records = self.cursor.fetchall()

        return records

    def execute_query(self, query, commit=True):
        """
        :param query:
        ejecuta una consulta y no retorna nada
        """
        try:
            if self.cursor.closed:
                self.set_new_connection()

            # stk = inspect.stack()
            # save_log_info(stk[0][1], "Exec :" + query, stk[0][3])
            self.cursor.execute(query)
            if commit:
                self.conn.commit()
        except Exception as ex:
            raise ex

    def close_cursor(self):
        if not self.cursor.closed:
            self.cursor.close()

    def close_conn(self):
        self.close_cursor()
        self.conn.close()
