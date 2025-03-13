import sys, json, psycopg2, redis, decimal, base64
from psycopg2.extras import LogicalReplicationConnection
from time import sleep
from datetime import datetime

class ReplicacaoLeitura:
    def __init__(self, industria):
        print('Iniciando Replicação em ' + industria)
        with open('/opt/airflow/dags/scripts/industrias.json', 'r') as f:
            industrias = json.load(f)
        if industria not in industrias.keys():
            raise Exception('Industria não encontrada no arquivo de industrias')
        acesso64 = industrias[industria]
        industria = base64.b64decode(acesso64).decode('utf-8')
        industria = json.loads(industria)
        self.industria = industria['industria']
        self.con_replication = psycopg2.connect(
            f"dbname='{industria['dbname']}' host='{industria['host']}' user='{industria['user']}' password='{industria['password']}'",
            connection_factory=LogicalReplicationConnection)
        self.con_origem_replication = psycopg2.connect(
            f"dbname='{industria['dbname']}' host='{industria['host']}' user='{industria['user']}' password='{industria['password']}'")
        self.cur_origem = self.con_origem_replication.cursor()
        self.con_changes = psycopg2.connect(
            f"dbname='base_destino' host='host_destino' user='{industria['user']}' password='{industria['password']}'") # alterar base e host
        self.con_changes.autocommit = True
        self.cur_changes = self.con_changes.cursor()

        self.cur_changes.execute(f"select string_agg(tabela, ',') from replicacoes where industria = '{self.industria}'") # criar uma tabela para armazenar as tabelas a serem replicadas
        self.list_tabelas = tuple(self.cur_changes.fetchone()[0].split(','))

        self.redis_conn = redis.Redis(host='servidor', port='6379', decode_responses=True)
        self.cur = self.con_replication.cursor()
        
        self.cols_destino = {}
        for t in self.list_tabelas:
            self.valida_cria_tabela(t)
            self.cur_changes.execute(f'''SELECT string_agg(column_name, ',') AS column_names 
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{t}';''')

            self.cols_destino[t] = tuple(self.cur_changes.fetchone()[0].split(','))
        
        self.create_slot() 
        self.iniciar_replicacao()

    def iniciar_replicacao(self):
        try:
            self.cur.consume_stream(self.consume)
        except psycopg2.DatabaseError as e:
            print(f"Erro na replicação: {e}")
            raise
        except Exception as e:
            print(f"Erro crítico: {e}")
            sys.exit(1)


    def valida_cria_tabela(self, table):
        self.cur_changes.execute(f"select count(1) from pg_tables where tablename = '{table}' ")
        qtd = self.cur_changes.fetchone()[0]

        if qtd > 0:
            return True
        else:
            self.cur_origem.execute(f"""select 'CREATE TABLE ' || max(table_name) || ' ( industria varchar(50), created_at_replica timestamp, updated_at_replica timestamp, operacao_replica varchar(10), ' ||
                string_agg(column_name || ' ' || udt_name ||
                case when udt_name = 'numeric' then '(' || numeric_precision || ', ' || numeric_scale || ')' 
                when udt_name = 'varchar' and character_maximum_length is not null then '(' || character_maximum_length || ')'
                else '' end
                || 
                CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE ' NULL' END, ', ') 
                || ' , CONSTRAINT ' || max(table_name) || '_pkey PRIMARY KEY (industria, id) ' || ' ) ' as createtable 
                from (
                SELECT table_name, column_name, character_maximum_length, numeric_precision, numeric_scale, case when column_name = 'id' then 'int4' else udt_name end, is_nullable 
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table}'
                ORDER BY ordinal_position ) x""")
            create = self.cur_origem.fetchone()
            if create :
                create = create[0]
            else:
                return False

            self.cur_changes.execute(create)
            self.con_changes.commit()

            self.cur_changes.execute(f" select 'grant all on {table} to \"' || usename || '\";' from pg_user ")
            grant = self.cur_changes.fetchall()
            if grant:
                for g in grant:
                    self.cur_changes.execute(g[0])
                    self.con_changes.commit()

            return True
        
    def create_slot(self):
        print('Iniciando Replicação em ' + self.industria)
        self.cur_origem.execute(f"""
            SELECT pg_terminate_backend(active_pid) 
            FROM pg_replication_slots 
            WHERE slot_name = 'replicacao_{self.industria}';
        """)
        self.con_origem_replication.commit()
        try:            
            self.cur.create_replication_slot(f'replicacao_{self.industria}', output_plugin='wal2json')
        except:
            pass
        self.cur.start_replication(slot_name=f'replicacao_{self.industria}', options={'pretty-print': 1}, decode=True)
        print('Replicação Iniciada')


    def consume(self, msg):
        dados = json.loads(msg.payload)
        mensagens_final = {}

        for x in dados.get('change', []):
            table = x.get('table')
            if table in self.list_tabelas:
                tipo = x.get('kind')

                if table not in mensagens_final:
                    mensagens_final[table] = {'colunas': '', 'insert': [], 'delete': []}

                sql_insert = ''
                if 'columnnames' in x:
                    col = tuple(x.get('columnnames'))
                    valores = tuple(x.get('columnvalues'))

                    padrao_colunas = ('industria', 'created_at_replica', 'updated_at_replica', 'operacao_replica') # colunas padrões na tabela destino
                    padrao_valores = (f"'{self.industria}'", "now()", "now()", f"'{tipo}'")

                    origem_columns = col + padrao_colunas
                    destino_columns = self.cols_destino[table]

                    valores_final = [self.limpar_dados(val) for val in valores] + list(padrao_valores)
                    colunas_e_valores = [(coluna, valor) for coluna, valor in zip(origem_columns, valores_final) if coluna in destino_columns]

                    colunas = ', '.join(coluna for coluna, _ in colunas_e_valores)
                    valores = ', '.join(valor for _, valor in colunas_e_valores)

                    mensagens_final[table]['colunas'] = colunas
                    sql_insert = f'({valores})'
                
                sql_delete = ''
                if 'oldkeys' in x:
                    chave = x['oldkeys']['keynames']
                    idDeletado = x['oldkeys']['keyvalues']
                    where_final = ' AND '.join(f'{k}={idDeletado[i]}' for i, k in enumerate(chave))
                    sql_delete = f'DELETE FROM {table} WHERE {where_final};'

                if tipo == 'insert':
                    mensagens_final[table]['insert'].append(sql_insert)
                elif tipo == 'update':
                    mensagens_final[table]['delete'].append(sql_delete)
                    mensagens_final[table]['insert'].append(sql_insert)
                elif tipo == 'delete':
                    mensagens_final[table]['delete'].append(sql_delete)

        if mensagens_final:
            self.redis_conn.rpush(f'sql_queue_{self.industria}', json.dumps([mensagens_final]))

        msg.cursor.send_feedback(flush_lsn=msg.data_start)


        
    def limpar_dados(self, n):
        if isinstance(n, list):
            lista = '['
            for m in n:
                dado = str(m)
                if not (type(m) == int or type(m) == float or type(m) == decimal.Decimal):
                    dado = self.limpar_dados(m)
                lista += dado + ','
            lista = lista[:-1] + ']'
            return "ARRAY%s" % str(lista)
        elif(type(n) == datetime):
            return f"'{str(n)[:23]}'"
        else:
            return "'%s'" % str(n).replace("'", "''") if str(n) != 'None' else 'NULL'


class ReplicacaoEscrita:
    def __init__(self, industria):
        print(f'Iniciando Replicação Escrita em {industria}')

        # Carregar credenciais da indústria com tratamento de erro
        try:
            with open('/opt/airflow/dags/scripts/industrias.json', 'r') as f:
                industrias = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise Exception(f'Erro ao carregar industrias.json: {e}')

        if industria not in industrias:
            raise Exception(f'Indústria "{industria}" não encontrada no arquivo de industrias.json')

        acesso64 = industrias[industria]
        decoded_data = base64.b64decode(acesso64).decode('utf-8')

        try:
            industria_data = json.loads(decoded_data)
        except json.JSONDecodeError:
            raise Exception('Erro ao decodificar credenciais da indústria.')

        self.industria = industria_data['industria']
        
        # Configurar conexão com PostgreSQL
        try:
            self.con_changes = psycopg2.connect(
                dbname='base_destino', # alterar
                host='host_destino', # alterar
                user=industria_data['user'],
                password=industria_data['password']
            )
            self.con_changes.autocommit = True
            self.cur_changes = self.con_changes.cursor()
        except psycopg2.Error as e:
            raise Exception(f'Erro ao conectar ao PostgreSQL: {e}')

        # Configurar conexão com Redis
        try:
            self.redis_conn = redis.Redis(host='servidor_redis', port=6379, decode_responses=True) # alterar
        except redis.RedisError as e:
            raise Exception(f'Erro ao conectar ao Redis: {e}')

        # Processar a fila do Redis
        self.process_queue(f'sql_queue_{self.industria}')
        
    def process_queue(self, queue_key):
        while True:
            try:
                item = self.redis_conn.blpop(queue_key, timeout=5)  # Timeout para evitar bloqueio eterno
                if item is None:
                    continue  # Nenhum item na fila, reitera o loop

                _, ajustes_json = item

                try:
                    ajustes = json.loads(ajustes_json)
                except json.JSONDecodeError:
                    print('Erro ao decodificar JSON da fila Redis.')
                    continue

                step_del = 100
                step_ins = 1000

                # Dicionário para agrupar mensagens por key
                mensagens_agrupadas = {}

                # Agrupar mensagens
                for ajuste in ajustes:
                    for key, mensagem in ajuste.items():
                        if key not in mensagens_agrupadas:
                            mensagens_agrupadas[key] = {
                                "colunas": mensagem["colunas"],
                                "insert": [],
                                "delete": []
                            }
                        mensagens_agrupadas[key]["delete"].extend(mensagem.get("delete", []))
                        mensagens_agrupadas[key]["insert"].extend(mensagem.get("insert", []))

                # Processar os DELETEs agrupados
                for key, mensagem in mensagens_agrupadas.items():
                    for index in range(0, len(mensagem["delete"]), step_del):
                        deletes = '\n'.join(mensagem["delete"][index:index+step_del])
                        self.execute_sql(deletes)

                # Processar os INSERTs agrupados
                for key, mensagem in mensagens_agrupadas.items():
                    if mensagem["colunas"]:  # Evita tentar inserir sem colunas definidas
                        for index in range(0, len(mensagem["insert"]), step_ins):
                            values = ', '.join(mensagem["insert"][index:index+step_ins])
                            insert = f"INSERT INTO {key} ({mensagem['colunas']}) VALUES {values} ON CONFLICT DO NOTHING;"
                            self.execute_sql(insert)

            except redis.RedisError as e:
                print(f'Erro no Redis: {e}')
                sleep(1)

    def execute_sql(self, sql):
        """Executa uma query SQL diretamente no banco de dados."""
        try:
            self.cur_changes.execute(sql)
        except psycopg2.Error as e:
            self.con_changes.rollback()
            print(f'Erro na query: {sql}\nDetalhes do erro: {e}')
            raise
