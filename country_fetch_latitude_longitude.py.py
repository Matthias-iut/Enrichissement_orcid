import requests
import xml.etree.ElementTree as ET
import pycountry
import csv
import time

# Liste des codes ISO des pays
country_codes = ["DE", "FR", "UK", "US", "ZZ"]  # ZZ est un exemple de code invalide

# URL de l'API Nominatim
base_url = "https://nominatim.openstreetmap.org/search"

# En-têtes HTTP
headers = {
    'User-Agent': 'MyGeoApp/1.0 (myemail@example.com)'
}

# Stocker les résultats
coordinates = []

for code in country_codes:
    try:
        # Gérer les cas spéciaux ou vérifier si le code est valide
        if code == "UK":
            country_name = "United Kingdom"
        else:
            country = pycountry.countries.get(alpha_2=code)
            if country is not None:
                country_name = country.name
            else:
                raise ValueError(f"Code de pays invalide ou inconnu : {code}")
        
        print(f"Recherche pour {code} ({country_name})...")
    except ValueError as e:
        print(e)
        coordinates.append({"Code": code, "Country": "Invalid Code", "Latitude": None, "Longitude": None})
        continue

    # Effectuer une requête GET à l'API pour obtenir des données XML
    params = {
        'q': country_name,
        'format': 'xml',
        'limit': 1  # Obtenir uniquement le meilleur résultat
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            # Analyse du XML
            root = ET.fromstring(response.content)
            place = root.find("place")
            if place is not None:
                lat = place.get("lat")
                lon = place.get("lon")
                print(f"Résultat pour {country_name}: lat={lat}, lon={lon}")
                coordinates.append({"Code": code, "Country": country_name, "Latitude": lat, "Longitude": lon})
            else:
                print(f"Aucun résultat trouvé pour {country_name}")
                coordinates.append({"Code": code, "Country": country_name, "Latitude": None, "Longitude": None})
        else:
            print(f"Erreur HTTP {response.status_code} pour {country_name}")
            coordinates.append({"Code": code, "Country": country_name, "Latitude": None, "Longitude": None})
    except requests.exceptions.RequestException as e:
        print(f"Erreur de requête pour {country_name}: {e}")
        coordinates.append({"Code": code, "Country": country_name, "Latitude": None, "Longitude": None})

    # Respecter les limites de l'API (1 requête/s)
    time.sleep(1)

# Enregistrer les résultats dans un fichier CSV
output_file = "country_coordinates.csv"
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=["Code", "Country", "Latitude", "Longitude"])
    writer.writeheader()
    writer.writerows(coordinates)

print(f"Fichier CSV généré : {output_file}")
