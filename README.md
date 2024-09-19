# Introduction

Challenge of creating an ETL process in Python to build a database that consolidates information on historical quotes from Bovespa.

# Extraction

The ETL process returns data on the performance of shares traded on the National Stock Exchange (B3), available at the link: https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/

The database was taken from the B3 historical series tab, in which the years 2021, 2022 and 2023 were selected.

Since it is a positional file, the file comes as a string without a delimiter, therefore, it was necessary to use the *colspecs* option of the *read_fwf* function of the *pandas* library to specify the number of characters in each column.

As a basis for the steps to be followed, the content available at the link: https://www.youtube.com/watch?v=gew9014pGaM

In order to interpret the file, a layout of the historical quotes file is available on the Bovespa website, which is available together with the project files.

First of all, it is worth noting that the files used in the base code differ from the files imported into Github, since the originals exceed the size allowed by this platform (25MB). Therefore, in order to exemplify, a *.txt* file was imported, called *example_database.TXT*.

# Transformation

The first step to be taken is to import the libraries that will be used throughout the project. In this case, it was only necessary to import the pandas library.

Subsequently, a string was created with the path where the files are located on the machine, a list with the columns of positions of each field and a list with the names of each field, all used as arguments of the *read_fwf* function.

**IMPORTANT** to note that, since we did not want the first line of the layout to be read, the argument *skiprows =1* was used.

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

The next step brings a function that filters the BDI code of the listed shares. In this case, only code number 2 was selected, corresponding to "Standard Lot".

```
def filter_stocks(df):
    df = df [df['codbdi']== 2]
    df = df.drop(['codbdi'], 1)

    return df
```

In order to adjust the date field, the following function was used:

```
def parse_date(df):
    df['data_pregao'] = pd.to_datetime(df['data_pregao'], format = '%Y%m%d')
    return df 
```

To complete the transformation process, the numeric fields were adjusted and divided by one hundred, so that they returned two decimal places after the comma and a transformation was made to be converted to the float type.

```
def parse_values(df):
    df['preco_abertura'] = (df['preco_abertura'] /100).astype(float)
    df['preco_maximo'] = (df['preco_maximo'] /100).astype(float)
    df['preco_minimo'] = (df['preco_minimo'] /100).astype(float)
    df['preco_fechamento'] = (df['preco_fechamento'] /100).astype(float)

    return df 
```

Aiming at possible automation and seeking good data engineering practices, all the steps of the process were created in the form of functions. Finally, the *save_files* function is used, which calls all the previous ones and can be used within a *for* loop to execute the process multiple times, just changing the year of the file and saving all the outputs in a folder of the operating system, in the *.csv* format.

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
