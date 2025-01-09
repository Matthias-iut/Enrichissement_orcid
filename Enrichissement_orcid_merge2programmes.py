import pandas as pd
from sqlalchemy import create_engine
import re

# Informations de connexion
db_config = {
    'server': '82.65.62.161',
    'port': 1433,
    'user': 'sae',
    'password': 'Cataldi123',
    'database': 'dblp_data',
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
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 17:02:32 2025

@author: mtrupin
"""

import requests
import csv
from xml.etree import ElementTree as ET

def fetch_address(orcid_id):
    """
    Récupère les adresses associées à un ORCID.
    """
    url = f"https://pub.orcid.org/v3.0/{orcid_id}"
    headers = {"Accept": "application/json"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Extraction de l'adresse
        addresses = data.get("person", {}).get("addresses", {}).get("address", [])
        city = addresses[0].get("city", {}).get("value") if addresses else None
        country = addresses[0].get("country", {}).get("value") if addresses else None

        return {"city": city, "country": country}

    except Exception as e:
        print(f"Erreur lors de la récupération de l'adresse pour ORCID {orcid_id}: {e}")
        return {"city": None, "country": None}


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

    # Extraire les informations personnelles (city et country)
    city = safe_find(root, ".//address:address/common:city", namespaces={"address": "http://www.orcid.org/ns/address", "common": "http://www.orcid.org/ns/common"})
    country = safe_find(root, ".//address:address/address:country", namespaces={"address": "http://www.orcid.org/ns/address"})

    # Extraire les distinctions
    distinctions_data = []
    for distinction in root.findall(
        ".//activities:distinction",
        namespaces={"activities": "http://www.orcid.org/ns/activities"}
    ):
        title = safe_find(distinction, ".//common:title", namespaces={"common": "http://www.orcid.org/ns/common"})
        if title:
            distinctions_data.append(title)

    # Extraire les employments et les mettre en colonnes
    employment_columns = {}
    for i, employment in enumerate(root.findall(
        ".//employment:employment-summary",
        namespaces={"employment": "http://www.orcid.org/ns/employment"}
    ), start=1):
        employment_columns[f"employment_{i}_role_title"] = safe_find(employment, ".//common:role-title", namespaces={"common": "http://www.orcid.org/ns/common"})
        employment_columns[f"employment_{i}_department_name"] = safe_find(employment, ".//common:department-name", namespaces={"common": "http://www.orcid.org/ns/common"})
        employment_columns[f"employment_{i}_organization_name"] = safe_find(employment, ".//common:name", namespaces={"common": "http://www.orcid.org/ns/common"})
        employment_columns[f"employment_{i}_start_date"] = safe_find(employment, ".//common:start-date", namespaces={"common": "http://www.orcid.org/ns/common"})
        # Ajouter la ville et le pays si disponibles
        employment_columns[f"employment_{i}_city"] = safe_find(employment, ".//common:city", namespaces={"common": "http://www.orcid.org/ns/common"})
        employment_columns[f"employment_{i}_country"] = safe_find(employment, ".//common:country", namespaces={"common": "http://www.orcid.org/ns/common"})
    
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
            "organization_name": safe_find(education, ".//common:name", namespaces={"common": "http://www.orcid.org/ns/common"})
        }
        educations.append(education_info)

    # Ajouter les informations principales et les colonnes d'emploi
    combined_data = {
        "name": name,
        "city": city,
        "country": country,
        "distinctions": ", ".join(distinctions_data),
        **employment_columns
    }

    # Ajouter les informations d'éducation dans des colonnes distinctes
    for i, education in enumerate(educations, 1):
        for key, value in education.items():
            combined_data[f"education_{i}_{key}"] = value

    return [combined_data]


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

        # Récupérer l'adresse (city et country)
        address_data = fetch_address(orcid_id)

        # Récupérer les données XML pour emploi, distinctions, etc.
        xml_data = get_orcid_data(orcid_id)
        if xml_data:
            employments_data = parse_employments_and_distinctions_and_educations(xml_data)
            for employment in employments_data:
                # Ajouter les données d'adresse à chaque ligne
                employment["ORCID"] = orcid_id
                employment["city"] = address_data.get("city")
                employment["country"] = address_data.get("country")
                all_employment_data.append(employment)

    return all_employment_data




# Liste des ORCID à tester
orcid_ids = [
   '0000-0002-0142-7810', '0000-0002-1411-5744', '0000-0002-7897-0520', 
   '0000-0002-3803-8368', '0000-0002-4461-7307', '0000-0002-7521-5781', 
   '0000-0003-0992-7948', '0000-0002-7521-5781', '0000-0003-3043-0450', 
   '0000-0002-2211-9382', '0000-0003-3349-4951', '0000-0003-3349-4951', 
   '0000-0003-3349-4951', '0000-0003-3349-4951', '0000-0002-7015-7359', 
   '0000-0002-9819-7221', '0000-0003-4187-5877', '0009-0007-6641-9735', 
   '0000-0002-3090-1059', '0000-0002-4201-8757', '0000-0001-5931-5566', 
   '0000-0003-4943-9324', '0000-0003-4713-5327', '0000-0002-2798-0104', 
   '0000-0002-7593-8509', '0000-0002-7002-5815', '0000-0003-4317-971X', 
   '0000-0001-5931-5566', '0000-0003-2324-238X', '0009-0009-4479-5371', 
   '0000-0002-1037-0588', '0000-0003-0097-1010', '0000-0001-6768-1478', 
   '0000-0002-8126-1717', '0000-0002-8126-1717', '0000-0001-8293-8227', 
   '0000-0003-4522-5804', '0000-0002-7385-5689', '0000-0002-5770-934X', 
   '0000-0002-1876-1583', '0000-0002-7869-6373', '0000-0002-4500-9098', 
   '0000-0002-4022-9796', '0000-0003-4082-5616', '0000-0002-9841-5292', 
   '0000-0002-9606-5751', '0009-0001-8974-1045', '0000-0002-9741-3463', 
   '0000-0002-9741-3463', '0009-0001-8974-1045', '0000-0003-1047-2143', 
   '0000-0002-2367-2219', '0000-0002-6170-4912', '0000-0002-8252-5278', 
   '0000-0002-8037-1949', '0000-0002-7268-1795', '0000-0002-7268-1795', 
   '0000-0002-6080-8170', '0000-0002-6775-753X', '0000-0002-2715-6000', 
   '0000-0003-1484-539X', '0000-0002-4201-8757', '0000-0003-3368-2215', 
   '0000-0001-9208-5336', '0000-0002-1637-0589', '0000-0002-3800-0757', 
   '0000-0002-0951-2358', '0000-0002-3128-9025', '0000-0003-1463-4680', 
   '0000-0003-3246-8364', '0000-0003-2498-9661', '0000-0002-9741-3463', 
   '0000-0002-1214-1009', '0000-0001-7604-8834', '0000-0002-1214-1009', 
   '0000-0001-7604-8834', '0000-0002-8217-2230', '0000-0002-1214-1009', 
   '0000-0001-7604-8834', '0000-0002-7107-8644', '0000-0001-5842-9991', 
   '0000-0001-7472-6603', '0000-0002-8028-4730', '0000-0003-0605-1282', 
   '0000-0002-7268-1795', '0000-0003-0605-1282', '0000-0003-0605-1282', 
   '0000-0002-2403-1683', '0000-0003-0605-1282', '0000-0002-7268-1795', 
   '0000-0002-2403-1683', '0000-0002-7268-1795', '0000-0002-0698-0922', 
   '0000-0001-7276-6097', '0000-0001-6052-0559', '0000-0002-5088-1462', 
   '0000-0002-1747-9914', '0000-0002-4635-9102', '0000-0003-0105-7730', 
   '0000-0002-5547-9739'
]

"""
orcid_ids = [
    "0000-0002-9834-9600",
    "0000-0002-4253-2588",
    "0000-0002-9081-2170",
    "0000-0002-1023-8325",
    "0000-0002-9548-7872",
    "0000-0002-8472-3147",
    "0000-0002-9834-9600",
    "0000-0002-9548-7872",
    "0000-0002-9834-9600",
    "0000-0003-2661-533X",
    "0000-0002-2277-7035",
    "0000-0003-2129-8910",
    "0000-0001-9239-4964",
    "0000-0002-4451-915X",
    "0000-0003-1430-6935",
    "0000-0001-9994-1764",
    "0000-0003-4711-2344",
    "0000-0003-0041-7431",
    "0000-0001-6175-9676",
    "0000-0003-1765-3502",
    "0000-0003-4943-3969",
    "0000-0003-3012-1412",
    "0000-0003-0982-4092",
    "0000-0002-2272-3248",
    "0000-0002-5687-6998",
    "0000-0002-4451-915X",
    "0000-0001-5240-6588",
    "0000-0003-4711-2344",
    "0000-0002-4451-915X",
    "0000-0002-3388-0054",
    "0000-0003-1765-3502",
    "0000-0002-0862-1974",
    "0000-0002-3647-7041",
    "0000-0003-4711-2344",
    "0000-0002-2336-0848",
    "0000-0001-7819-4202",
    "0000-0003-0131-8961",
    "0000-0001-7987-6459",
    "0000-0002-5281-8221",
    "0000-0001-5240-6588",
    "0000-0002-2830-9291",
    "0000-0002-6294-7051",
    "0000-0003-0041-7431",
    "0000-0002-3647-7041",
    "0000-0002-2915-720X",
    "0000-0001-7819-4202",
    "0000-0003-4711-2344",
    "0000-0001-8530-8431",
    "0000-0002-8117-7064",
    "0000-0002-2819-8827",
    "0000-0002-4642-7064",
    "0000-0002-3156-6483",
    "0000-0002-0862-1974",
    "0000-0001-6808-4448",
    "0000-0001-8607-8025",
    "0000-0002-8613-7467",
    "0000-0002-4451-915X",
    "0000-0002-7399-7783",
    "0000-0002-4642-7064",
    "0000-0002-3156-6483",
    "0000-0003-1722-5188",
    "0000-0003-2566-7146",
    "0000-0003-4711-2344",
    "0000-0001-5240-6588",
    "0000-0001-6175-9676",
    "0000-0003-4711-2344",
    "0000-0001-7270-5817",
    "0000-0001-8479-0262",
    "0000-0001-7819-4202",
    "0000-0001-9052-100X",
    "0000-0001-9002-5874",
    "0000-0002-9029-5185",
    "0000-0002-7399-2712",
    "0000-0001-5017-9473",
    "0000-0003-2052-5536",
    "0000-0001-7819-4202",
    "0000-0002-1128-2483",
    "0000-0002-7109-1689",
    "0000-0001-6175-9676",
    "0000-0001-6808-4448",
    "0000-0001-6753-4929",
    "0000-0002-6699-1818",
    "0000-0001-9907-084X",
    "0000-0003-3049-477X",
    "0000-0002-8999-574X",
    "0000-0003-2240-9918",
    "0000-0003-1889-2791",
    "0000-0002-0988-5672",
    "0000-0001-8530-8431",
    "0000-0002-8145-3348",
    "0000-0002-4642-7064",
    "0000-0001-7270-5817",
    "0000-0001-7044-4721",
    "0000-0002-2272-3248",
    "0000-0001-8950-5424",
    "0000-0003-4711-2344",
    "0000-0002-5518-8990",
    "0000-0001-6753-4929",
    "0000-0003-4133-6419",
    "0000-0002-5164-4004"
]
"""
"""
orcid_ids = [
    "0000-0002-1825-0097",  # ORCID valide avec des données publiques
    "0000-0003-1419-2405",  # ORCID de test
    "0000-0002-4581-6092",
    "0000-0001-9495-7660",
    "0000-0002-4353-2424",
    "0000-0001-7284-5385",
    "0000-0003-1727-8984"
]
"""
all_employment_data = process_orcid_ids(orcid_ids)

# Sauvegarder dans un fichier CSV
save_to_csv("orcid_employment_data_with_names_distinctions_educations.csv", all_employment_data)
