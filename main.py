from google.cloud import storage

PROJECT_ID="PROJECT_ID"
BUCKET_NAME = "YOUR BUCKET NAME"

storage_client = storage.Client(project=PROJECT_ID)
buckets = storage_client.list_buckets()

buckets = storage_client.list_buckets()
for bucket in buckets:
    print(f"*****Bucket Name: {bucket.name} \nlocalisation : {bucket.location} : \nlocation_type: {bucket.location_type}\n")


blobs = storage_client.list_blobs(BUCKET_NAME)
for blob in blobs:
    print(blob.name)


def upload_blob(bucket_name, fichier_a_transferer, le_nom_du_fichier_dans_le_cloud):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(le_nom_du_fichier_dans_le_cloud)
    blob.upload_from_filename(fichier_a_transferer)
    print(f"File {fichier_a_transferer} uploaded to {le_nom_du_fichier_dans_le_cloud}.")

exemple_de_fichier = "files/fichier_test.csv"

upload_blob(BUCKET_NAME, exemple_de_fichier, "fichier_test.csv")

def download_file(bucket_name, nom_de_lobjet_telecharger, nom_en_local):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(nom_de_lobjet_telecharger)

    # Download the blob to a file
    blob.download_to_filename(nom_en_local)
    print(f"File {nom_de_lobjet_telecharger} downloaded to {nom_en_local}.")


def copier_file_to(blob, folder):
    ### Compléter le code

def supprimer_object(blob):
    ### Compléter le code

def process_file(blob):
    data = blob.download_as_text()
    try:
        df = pd.read_csv(io.StringIO(data))
        ## Tester la conformité du fichier
        ### Si tout est clean copier_file_to(blob, folder="clean") puis supprimer_object(blob)
        ### S'il ya des erreurs copier_file_to(blob, folder="error") puis supprimer_object(blob)
    except:
        ### traiter les erreurs et si ca vient du fichier copier_file_to(blob, folder="error")

def process_files(bucket_name):
    blobs = lister_les_objects(bucket_name, prefix=None, delimiter=None)
    for blob in blobs:
        process_file(blob)


download_file(BUCKET_NAME, "fichier_test.csv", "fichier_test.csv")
