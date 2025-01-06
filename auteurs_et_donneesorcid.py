import requests
import csv

# Fonction pour récupérer le nom de famille et l'adresse depuis l'API ORCID
def fetch_family_name_and_address(orcid_id):
    url = f"https://pub.orcid.org/v3.0/{orcid_id}"
    headers = {"Accept": "application/json"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Vérifie les erreurs HTTP
        data = response.json()
        
        # Extraction du nom de famille
        name = data.get("person", {}).get("name", {})
        family_name = name.get("family-name", {}).get("value", "")
        
        # Extraction des adresses
        addresses = data.get("person", {}).get("addresses", {}).get("address", [])
        countries = [addr.get("country", {}).get("value") for addr in addresses]

        return {
            "ORCID": orcid_id,
            "Family Name": family_name,
            "Address": ", ".join(countries) if countries else "Non disponible"
        }
    
    except Exception as e:
        print(f"Erreur lors de la récupération des données pour ORCID {orcid_id}: {e}")
        return {"ORCID": orcid_id, "Family Name": "", "Address": "Erreur"}

# Liste des identifiants ORCID
orcid_ids = [
    "0000-0002-1825-0097",
    "0000-0001-5109-3700",
    "0000-0003-1419-2405"
]

# Création du tableau avec les données
table_data = []
for orcid_id in orcid_ids:
    table_data.append(fetch_family_name_and_address(orcid_id))

# Vérification des clés du dictionnaire
fieldnames = ["ORCID", "Family Name", "Address"]

# Export des données dans un fichier CSV
output_file = "orcid_family_name_and_address.csv"
with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(table_data)

print(f"Les données ont été exportées dans le fichier {output_file}.")




