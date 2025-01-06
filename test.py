
import pandas as pd
from sqlalchemy import create_engine
import re

# Informations de connexion
db_config = {
    'server': '82.65.62.161',
    'port': 1433,
    'user': 'sa',
    'password': 'SaeCataldi!',
    'database': 'master',
}

# Création de l'engine SQLAlchemy
try:
    engine = create_engine(
        f"mssql+pymssql://{db_config['user']}:{db_config['password']}@{db_config['server']}:{db_config['port']}/{db_config['database']}"
    )
    conn = engine.connect()
    print("Connexion réussie avec SQLAlchemy !")
except Exception as e:
    print("Erreur lors de la connexion :", e)
    exit()

# Lecture des données avec une requête adaptée à SQL Server
query = "SELECT TOP 10 Authors FROM dblp4;"  # Utilisation de TOP pour SQL Server
try:
    original_df = pd.read_sql_query(query, conn)
    print("Données lues avec succès !")
except Exception as e:
    print("Erreur lors de l'exécution de la requête :", e)
    conn.close()
    exit()

# Fonction pour parser les auteurs et leurs ORCID
def parse_authors(authors_str):
    authors = []
    for author in authors_str.split(';'):
        author = author.strip()
        match = re.match(r'^(.*?)(?:\(ORCID:\s*([\d-]+)\))?$', author)
        if match:
            name = match.group(1).strip()
            orcid = match.group(2).strip() if match.group(2) else None
            authors.append((name, orcid))
    return authors

# Extraction et transformation des données
all_authors = []
for authors_str in original_df['Authors']:
    all_authors.extend(parse_authors(authors_str))

# Création d'un DataFrame avec les données extraites
authors_df = pd.DataFrame(all_authors, columns=['FullName', 'ORCIDNumber'])

print(authors_df)
"""
# Optionnel : Création de la table finale et insertion des données
try:
    with conn.begin() as transaction:
        # Création de la table si elle n'existe pas
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS Authors (
            FullName NVARCHAR(255),
            ORCIDNumber NVARCHAR(255)
        );
        '''
        transaction.execute(create_table_query)

        # Insertion des données dans la table Authors
        authors_df.to_sql('Authors', con=transaction, if_exists='append', index=False)
        print("Données insérées avec succès !")

        # Validation : affichage des résultats
        validation_query = "SELECT * FROM Authors;"
        final_df = pd.read_sql_query(validation_query, transaction)
        print("Données dans la table Authors :")
        print(final_df)
except Exception as e:
    print("Erreur lors de l'insertion ou de la validation des données :", e)
"""
# Fermeture de la connexion
conn.close()
