# Desafio de Estágio Pragma: Processamento de Logs do Quake III Arena 

## Resumo da solução

&emsp;Este projeto foi desenvolvido como parte do **desafio técnico do programa de estágio da Pragma** e tem como objetivo processar o log do jogo **Quake III Arena**, transformando eventos brutos em dados estruturados e analisáveis.

&emsp;A solução percorre o arquivo de log de forma sequencial, identifica cada partida a partir do evento `InitGame:`, extrai o mapa, contabiliza o total de mortes e consolida estatísticas individuais dos jogadores. Durante o parsing, o código trata particularidades do log real, como timestamps no início das linhas, mudanças de nome ao longo da partida e diferentes tipos de morte.

&emsp;Mortes causadas pelo ambiente (`<world>`) são tratadas como mortes ambientais, suicídios são identificados corretamente e apenas eliminações válidas contam como kills do jogador. Ao final, os dados são exportados em **JSON**, convertidos para **Parquet** e utilizados para gerar análises simples em txt mesmo, garantindo um dataset consistente e fácil de validar.

---

## Forma de execução

### Pré-requisitos

- Python 3.10 ou superior  
- Biblioteca adicional:
  ```python
  pip install pyarrow
  ```
### Executando o script

Coloque o arquivo de log na pasta data (já está lá orginalmente)

#### Execute o comando:

```python
python main.py 
```
O script aceita automaticamente arquivos .txt ou .log.

### Saídas geradas

`output/games.json` — dados estruturados das partidas

`output/games.parquet` — mesma informação em formato Parquet

`output/analyses.txt` — resumo das análises do dataset