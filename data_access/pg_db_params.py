"""
Created on 10 oct. 2018

@author: egaudenti
"""


class PgDbParams(object):
    """
    Clase que contiene la definicion de los parametros necesarios para una conn 
    """

    def __init__(self, dic_sv):  # p_host="localhost", p_user="postgres", p_password="", p_port=5432, p_db_name=None, p_desc_name=None):

        self.host = dic_sv["PG_HOST"]  # p_host
        self.user = dic_sv["PG_USER"]  # p_user
        self.password = dic_sv["PG_PASS"]  # p_password
        self.port = dic_sv["PG_PORT"]  # int(p_port)
        self.db_name = dic_sv["PG_DB_NAME"]

    def set_all(self, p_host=None, p_user=None, p_password=None, p_port=None, p_dbname=None):
        """
        Funcion que permite setear cualquier atributo nuevamente, pasar por parametro el/los nuevo/s atributo/s
        """
        if p_host:
            self.host = p_host
        if p_user:
            self.user = p_user
        if p_password:
            self.password = p_password
        if p_port:
            self.port = p_port
        if p_dbname:
            self.db_name = p_dbname

    def get_conn_string(self):
        conn = "host='{}'  user='{}' password='{}' port={} ".format(self.host, self.user, self.password, self.port)

        if self.db_name:
            conn += "dbname='{}'".format(self.db_name)

        return conn

    def __str__(self):
        return self.get_conn_string()
