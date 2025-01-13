import csv
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut


def get_coordinates(city, country, geolocator, retries=3):
    """Obtient les coordonnées d'une ville et d'un pays."""
    try:
        location = geolocator.geocode(f"{city}, {country}")
        if location:
            return location.latitude, location.longitude
    except GeocoderTimedOut:
        if retries > 0:
            return get_coordinates(city, country, geolocator, retries - 1)
    return None, None


def process_csv(input_file, output_file):
    """Lit un fichier CSV d'entrée, ajoute les coordonnées, et écrit un fichier CSV de sortie."""
    geolocator = Nominatim(user_agent="geo-locator")

    with open(input_file, mode='r', encoding='utf-8') as infile, \
            open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
        reader = csv.DictReader(infile, delimiter=';')
        fieldnames = reader.fieldnames + ['Latitude', 'Longitude']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=';')

        writer.writeheader()

        for row in reader:
            city = row.get('City', '').strip()
            country = row.get('Country', '').strip()
            latitude, longitude = get_coordinates(city, country, geolocator)
            row['Latitude'] = latitude
            row['Longitude'] = longitude
            writer.writerow(row)


if __name__ == "__main__":
    input_file = "cities.csv"  # Nom du fichier d'entrée (ex: cities.csv)
    output_file = "cities_with_coordinates.csv"  # Nom du fichier de sortie
    process_csv(input_file, output_file)
    print(f"Les coordonnées ont été ajoutées dans {output_file}")
