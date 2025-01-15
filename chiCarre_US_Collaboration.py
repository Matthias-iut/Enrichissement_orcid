from sqlalchemy import create_engine, text
import pandas as pd
from scipy.stats import chi2_contingency

# Informations de connexion
db_config = {
    'server': '82.65.62.161',
    'port': 1433,
    'user': 'sae',
    'password': 'Cataldi123',
    'database': 'dblp4', # Mettre à jour la database
}

# Requête pour obtenir les données nécessaires
def get_data_for_chi_square(connection):
    query = """
    SELECT 
        p.person_id,
        c.country_name,
        COUNT(DISTINCT pr.publication_id) AS number_of_coeditions
    FROM 
        Person p
    JOIN 
        Contribuer cr ON p.person_id = cr.person_id
    GROUP BY 
        p.person_id, c.country_name
    """
    return pd.read_sql_query(query, connection)

# Calculer le test du Chi-carré
def perform_chi_square(data):
    # Créer une table de contingence : 
    #  - Les lignes : US ou non
    #  - Les colonnes : coédition ou non
    data['is_us'] = data['country_name'].apply(lambda x: 1 if x.lower() == 'us' else 0)
    data['is_coeditor'] = data['number_of_coeditions'].apply(lambda x: 1 if x > 1 else 0)

    contingency_table = pd.crosstab(data['is_us'], data['is_coeditor'])

    print("\nTable de contingence :")
    print(contingency_table)

    # Test du Chi-carré
    chi2, p, dof, expected = chi2_contingency(contingency_table)

    print("\nRésultats du test du Chi-carré :")
    print(f"Chi2 : {chi2}")
    print(f"p-value : {p}")
    print(f"Degrés de liberté : {dof}")
    print(f"Tableau des valeurs attendues :\n{expected}")

    # Interprétation
    if p < 0.05:
        print("\nConclusion : Il existe une relation significative entre le pays (US ou non) et le fait d'être impliqué dans une coédition.")
    else:
        print("\nConclusion : Aucune relation significative entre le pays (US ou non) et le fait d'être impliqué dans une coédition.")

# Fonction principale
def main():
    try:
        # Connexion à la base de données
        engine = create_engine(
            f"mssql+pymssql://{db_config['user']}:{db_config['password']}@{db_config['server']}:{db_config['port']}/{db_config['database']}"
        )
        connection = engine.connect()
        print("Connexion réussie avec SQLAlchemy !")

        # Récupérer les données
        data = get_data_for_chi_square(connection)
        print(f"\nDonnées récupérées : {data.shape[0]} lignes")

        # Exécuter le test du Chi-carré
        perform_chi_square(data)

    except Exception as e:
        print("Erreur lors de l'exécution :", e)

    finally:
        if 'connection' in locals():
            connection.close()
            print("Connexion fermée.")

# Exécution du script
if __name__ == "__main__":
    main()
