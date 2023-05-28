# Introdução

Desafio de criação de um processo de ETL em python, para construção de uma base de dados que consolida informações de cotações históricas do Bovespa.

# Extração

O processo de ETL retorna dados do desempenho das ações negociadas na Bolsa de Valores Nacional (B3), disponível no link: https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/ 

A base de dados foi retirada da aba séries históricas da B3, no qual foram selecionados os anos de 2021, 2022 e 2023.

Por tratar-se de um arquivo posicional, o arquivo vem como string sem delimitador, portanto, foi necessário utilizar a opção *colspecs* da função *read_fwf* da biblioteca *pandas*, para especificar o número de caracteres de cada coluna.

Como base para as etapas a serem seguidas, foi utilizado o conteúdo disponível no link: https://www.youtube.com/watch?v=gew9014pGaM

A fim de interpretar o arquivo, é disponibilizado no site da bovespa um layout do arquivo_cotações históricas, o qual está disponível juntamente com os arquivos do projeto.

Em um primeito momento, cabe salientar que os arquivos utilizados no código base diferem dos arquivos importados no Github, uma vez que os originais excedem o tamanho permitido por essa plataforma (25MB). Assim, visando exemplificar, foi importado um arquivo *.txt*, chamado *exemplo_base_dados.TXT*.

# Transformação

A primeira etapa a ser feita é importar as bibliotecas que serão utilizadas no decorrer do projeto. Nesse caso, somente foi preciso importar a biblioteca pandas.

Posteriormente, foram criadas uma string com o caminho em que os arquivos estão localizados na máquina, uma lista com as colunas de posições de cada campo e uma lista com os nomes de cada campo, todos utilizadas como argumentos da função *read_fwf*.

**IMPORTANTE** salientar que, por não querer que a primeira linha do layout fosse lida, foi utilizado o argumento *skiprows =1*.

```
def read_files(path, name_file, year_date, type_file):

    _file = f'{path}{name_file}{year_date}.{type_file}'

    colspecs = [
        (2,10),
        (10,12),
        (12,24),
        (27,39),
        (56,69),
        (69,82),
        (82,95),
        (108,121),
        (152,170),
        (170,188)
    ]

    names = ['data_pregao', 'codbdi', 'sigla_acao', 'nome_acao', 'preco_abertura', 'preco_maximo', 'preco_minimo', 'preco_fechamento', 'qtd_negocios', 'volume_negocios']

    df = pd.read_fwf(_file, colspecs = colspecs, names = names, skiprows =1)

    return df
```

O step seguinte traz uma função que realiza o filtro no código BDI das ações listadas. Nesse caso, foi selecionado somente o código de número 2, correspondente a "Lote Padrão".

```
def filter_stocks(df):
    df = df [df['codbdi']== 2]
    df = df.drop(['codbdi'], 1)

    return df
```

Visando ajustar o campo de data, foi utilizada a seguinte função:

```
def parse_date(df):
    df['data_pregao'] = pd.to_datetime(df['data_pregao'], format = '%Y%m%d')
    return df 
```

Para finalizar o processo de transformação, foram ajustados os campos numéricos e dividido por cem, para que retornassem duas casas decimais após a vírgula e feita uma transformação para ser convertido no tipo float.

```
def parse_values(df):
    df['preco_abertura'] = (df['preco_abertura'] /100).astype(float)
    df['preco_maximo'] = (df['preco_maximo'] /100).astype(float)
    df['preco_minimo'] = (df['preco_minimo'] /100).astype(float)
    df['preco_fechamento'] = (df['preco_fechamento'] /100).astype(float)

    return df 
```

Visando possível automatização e buscando boas práticas de engenharia de dados, todas as etapas do processo foram criadas no formato de funções. Por fim, é utilizada a função *save_files*, que chama todas as anteriores e pode ser utilizada dentro de um *loop for* para executar o processo multiplas vezes, apenas alterando o ano do arquivo e salvando todos os outputs em uma pasta do sistema operacional, no formato *.csv*.

```
def save_files(path, name_file, year_date, type_file):
    df = read_files(path, name_file, year_date, type_file)
    df = filter_stocks(df)
    df = parse_date(df)
    df = parse_values(df)
    df.to_csv(f'{path}//bovespa//{year_date}.csv', index=False)

year_date = ['2021', '2022', '2023']
path = <file_path>
name_file = 'COTAHIST_A'
type_file = 'txt'

for date in year_date:
    save_files(path, name_file, date, type_file)
```
