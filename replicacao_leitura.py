import sys
from replicacao import ReplicacaoLeitura
from time import sleep

if len(sys.argv) < 2:
    raise Exception('Industria não informada')
industria = sys.argv[1]

while True:
    ReplicacaoLeitura(industria)
    sleep(10)
