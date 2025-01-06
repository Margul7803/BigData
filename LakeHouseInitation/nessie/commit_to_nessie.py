def commit_to_nessie(api_url, commit_meta, operations):
    """
    Fonction pour effectuer un commit sur Nessie via l'API REST.

    :param api_url: URL de l'API Nessie (par exemple : "http://<nessie_host>:<port>/api/v2/trees/branch_name/history/commit")
    :param commit_meta: Metadata du commit (auteur, date, message, etc.)
    :param operations: Liste des opérations à réaliser (ex : PUT sur Delta Lake Tables)
    """
    # Création du body de la requête
    body = {
        "commitMeta": commit_meta,
        "operations": operations
    }

    # En-têtes de la requête HTTP
    headers = {
        "Content-Type": "application/json"
    }

    # Effectuer l'appel API POST
    try:
        response = requests.post(
            api_url, data=json.dumps(body), headers=headers)
        if response.status_code == 200:
            print("Commit effectué avec succès !")
            print(response.json())  # Afficher la réponse du serveur
        else:
            print(f"Erreur lors du commit : {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Erreur de connexion à l'API Nessie : {str(e)}")


# Exemple de données à envoyer (en fonction du corps donné dans la question)
commit_meta = {
    "author": "mario <mario.aboujamra@gmail.com>",
    "authorTime": datetime.utcnow().isoformat() + "Z",  # Utiliser l'heure UTC actuelle
    "message": "Initialise Bronze Layer",
    "properties": {
        "additionalProp1": "init"
    },
    "signedOffBy": "mario.aboujamra <mario.aboujamra@gmail.com>"
}

operations = [
    {
        "type": "PUT",
        "key": {
            "elements": ["alcohol_consumption_gdp"]
        },
        "content": {
            "type": "DELTA_LAKE_TABLE",
            "metadataLocation": "/mnt/conteneurmarioabjmb/bronze/alcohol_consumption_gdp/",
            "snapshotId": 1,
            "schemaId": 2,
            "specId": 3,
            "sortOrderId": 4
        }
    },
    {
        "type": "PUT",
        "key": {
            "elements": ["happiness_alcohol_consumption"]
        },
        "content": {
            "type": "DELTA_LAKE_TABLE",
            "metadataLocation": "/mnt/conteneurmarioabjmb/bronze/happiness_alcohol_consumption/",
            "snapshotId": 1,
            "schemaId": 2,
            "specId": 3,
            "sortOrderId": 4
        }
    },
    {
        "type": "PUT",
        "key": {
            "elements": ["alcohol_specific_deaths"]
        },
        "content": {
            "type": "DELTA_LAKE_TABLE",
            "metadataLocation": "/mnt/conteneurmarioabjmb/bronze/alcohol_specific_deaths/",
            "snapshotId": 1,
            "schemaId": 2,
            "specId": 3,
            "sortOrderId": 4
        }
    }
]

# Exemple d'appel de la fonction
api_url = "http://<nessie_host>:<port>/api/v2/trees/branch_name/history/commit"
commit_to_nessie(api_url, commit_meta, operations)
