#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 11:19:36 2025

@author: mtrupin
"""
import pandas as pd
import psycopg2  # Pour PostgreSQL. Utilisez `mysql.connector` pour MySQL.
import re

# Informations de connexion
db_config = {
    'host': '82.65.62.161',
    'port': 1433,  # Changez pour le port de votre base (3306 pour MySQL)
    'user': 'sa',
    'password': 'SaeCataldi!',
    'database': 'master',
}

# Connexion à la base de données
try:
    conn = psycopg2.connect(**db_config)  # Remplacez par `mysql.connector.connect(**db_config)` pour MySQL.
except Exception as e:
    print("Erreur lors de la connexion :", e)
    exit()

# Lecture des données
query = "SELECT Authors FROM dblp4 LIMIT 5000;"  # Assurez-vous que la table `OriginalTable` existe et est correcte.
original_df = pd.read_sql_query(query, conn)

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
# Création de la table finale et insertion des données
create_table_query = '''
CREATE TABLE IF NOT EXISTS Authors (
    FullName TEXT,
    ORCIDNumber TEXT
);
'''
with conn.cursor() as cursor:
    cursor.execute(create_table_query)
    conn.commit()

# Insertion des données dans la table Authors
insert_query = "INSERT INTO Authors (FullName, ORCIDNumber) VALUES (%s, %s);"
with conn.cursor() as cursor:
    cursor.executemany(insert_query, authors_df.values.tolist())
    conn.commit()

# Validation : affichage des résultats
final_df = pd.read_sql_query("SELECT * FROM Authors;", conn)
print(final_df)
"""
# Fermeture de la connexion
conn.close()