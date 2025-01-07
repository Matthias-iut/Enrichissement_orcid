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
query = "SELECT Authors FROM dblp4;"  # Utilisation de TOP pour SQL Server
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

#print(authors_df)

# Fermeture de la connexion
conn.close()

#Fetch des données ORCID
import requests
import csv
from xml.etree import ElementTree as ET

def get_orcid_data(orcid_id):
    """
    Récupère les données publiques d'un ORCID sous format XML.
    """
    url = f"https://pub.orcid.org/v3.0/{orcid_id}/activities"
    headers = {"Accept": "application/xml"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text  # Retourne les données XML sous forme de texte
    except requests.exceptions.HTTPError as e:
        print(f"Erreur HTTP {e.response.status_code} pour ORCID {orcid_id}")
    except requests.exceptions.RequestException as e:
        print(f"Erreur de connexion pour ORCID {orcid_id} : {e}")
    return None

def safe_find(element, path, namespaces, default=None):
    """
    Recherche un élément dans le XML de manière sécurisée (évite les erreurs si l'élément n'existe pas).
    """
    result = element.find(path, namespaces)
    return result.text if result is not None else default

def parse_employments_and_distinctions_and_educations(xml_data):
    """
    Extrait les données d'employments, distinctions, éducation et noms à partir du XML.
    """
    root = ET.fromstring(xml_data)
    
    # Extraire les informations personnelles (nom)
    name = safe_find(root, ".//person:name", namespaces={"person": "http://www.orcid.org/ns/person"})
    
    # Extraire les distinctions
    distinctions_data = []
    for distinction in root.findall(
        ".//activities:distinction",
        namespaces={"activities": "http://www.orcid.org/ns/activities"}
    ):
        title = safe_find(distinction, ".//common:title", namespaces={"common": "http://www.orcid.org/ns/common"})
        if title:
            distinctions_data.append(title)

    # Extraire les employments
    employments_data = []
    for employment in root.findall(
        ".//employment:employment-summary",
        namespaces={"employment": "http://www.orcid.org/ns/employment"}
    ):
        employment_info = {
            "role_title": safe_find(employment, ".//common:role-title", namespaces={"common": "http://www.orcid.org/ns/common"}),
            "department_name": safe_find(employment, ".//common:department-name", namespaces={"common": "http://www.orcid.org/ns/common"}),
            "organization_name": safe_find(employment, ".//common:name", namespaces={"common": "http://www.orcid.org/ns/common"}),
            "start_date": safe_find(employment, ".//common:start-date", namespaces={"common": "http://www.orcid.org/ns/common"}),
            "city": safe_find(employment, ".//common:city", namespaces={"common": "http://www.orcid.org/ns/common"}),
            "country": safe_find(employment, ".//common:country", namespaces={"common": "http://www.orcid.org/ns/common"}),
            "name": name,  # Ajouter le nom de la personne
            "distinctions": ", ".join(distinctions_data)  # Ajouter les distinctions
        }

        # Ajouter les informations d'éducation
        educations = []
        for education in root.findall(
            ".//education:education-summary",
            namespaces={"education": "http://www.orcid.org/ns/education"}
        ):
            education_info = {
                "degree_title": safe_find(education, ".//common:degree-name", namespaces={"common": "http://www.orcid.org/ns/common"}),
                "start_date": safe_find(education, ".//common:start-date", namespaces={"common": "http://www.orcid.org/ns/common"}),
                "end_date": safe_find(education, ".//common:end-date", namespaces={"common": "http://www.orcid.org/ns/common"}),
                "organization_name": safe_find(education, ".//common:name", namespaces={"common": "http://www.orcid.org/ns/common"}),
                "city": safe_find(education, ".//common:city", namespaces={"common": "http://www.orcid.org/ns/common"}),
                "country": safe_find(education, ".//common:country", namespaces={"common": "http://www.orcid.org/ns/common"})
            }

            educations.append(education_info)

        # Ajouter les informations d'éducation dans des colonnes distinctes
        for i, education in enumerate(educations, 1):
            for key, value in education.items():
                employment_info[f"education_{i}_{key}"] = value

        employments_data.append(employment_info)

    return employments_data

def save_to_csv(filename, data):
    """
    Sauvegarde les données extraites dans un fichier CSV.
    """
    if not data:
        print("Aucune donnée à écrire dans le fichier CSV.")
        return

    # Identification des clés (colonnes) uniques pour le CSV
    all_keys = {key for item in data for key in item.keys()}
    
    # Sauvegarde des données dans un fichier CSV
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=sorted(all_keys))
        writer.writeheader()
        writer.writerows(data)
    print(f"Données enregistrées dans {filename}")

def process_orcid_ids(orcid_ids):
    """
    Traite une liste d'ORCID pour récupérer et extraire les données.
    """
    all_employment_data = []

    for orcid_id in orcid_ids:
        print(f"Récupération des données pour ORCID {orcid_id}...")
        xml_data = get_orcid_data(orcid_id)
        if xml_data:
            employments_data = parse_employments_and_distinctions_and_educations(xml_data)
            for employment in employments_data:
                employment["ORCID"] = orcid_id  # Ajouter l'ORCID comme champ
                all_employment_data.append(employment)
                
    return all_employment_data


# Liste des ORCID à tester
orcid_ids = authors_df['ORCIDNumber']
"""[
        "0000-0002-1825-0097",  # ORCID valide avec des données publiques
        "0000-0003-1419-2405",  # ORCID de test
        "0000-0002-4581-6092",
        "0000-0001-9495-7660",
        "0000-0002-4353-2424",
        "0000-0001-7284-5385",
        "0000-0003-1727-8984"
]"""

all_employment_data = process_orcid_ids(orcid_ids)

# Sauvegarder dans un fichier CSV
save_to_csv("orcid_employment_data_with_names_distinctions_educations.csv", all_employment_data)
