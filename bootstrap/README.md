# Bootstrap

The Bootstrap Google Cloud functions allow a cluster of Transicator Change Servers to backup and restore from a
Google Cloud Store. This helps avoid a potential issue when scaling up the Change Server cluster where existing 
clients may attach to the new Change Server, receive a "snapshot too old" message because the new Change Server
doesn't have all necessary context, and force the clients to request new snapshots.

The algorithm at this point is simple: If the backup file age > minBackupAge configured on the bootstrap, the backup 
offered by the Change Server is accepted and stored. A future enhancement may be to consider a delta of changes made 
between backups or similar.

To set up, follow the instructions below and configure the Change Server with the appropriate URIs to reach the
backup and restore functions.

## Config
    
    cp config.json_example config.json
    
Edit config.json values to your liking. The values are: 

* `bucketName`: The GCS bucket that backups will be stored in  
* `minBackupAge`: The minimum age in milliseconds of a backup before replacing the new backup

## Install dependencies

    npm install

## Set env (change these values) 

    export PROJECT_ID=edgex-149918
    export CLASS=multi_regional
    export LOCATION=us
    export REGION=us-central1
    export SCRIPTS_BUCKET=gs://transicator-bootstrap-scripts
    export DATA_BUCKET=gs://transicator-bootstrap-data

Notes:
* `PROJECT_ID` is your Google Cloud Platform Project ID
* `SCRIPTS_BUCKET` and `DATA_BUCKET` must be globally unique bucket identifiers (although they could be the same bucket)
* The `bucketName` config value in your `config.json` and `DATA_BUCKET` value must match
* These env vars are not generally necessary, they're just to configure the following commands 

## Deploy

1. Create the Google Cloud Storage buckets:


    gsutil mb -c $CLASS -l $LOCATION -p $PROJECT_ID $SCRIPTS_BUCKET
    gsutil mb -c $CLASS -l $LOCATION -p $PROJECT_ID $DATA_BUCKET


2.  Deploy the Google Cloud Functions:


    gcloud --project $PROJECT_ID beta functions deploy bootstrapBackup --stage-bucket $SCRIPTS_BUCKET --trigger-http --timeout 180s
    gcloud --project $PROJECT_ID beta functions deploy bootstrapRestore --stage-bucket $SCRIPTS_BUCKET --trigger-http --timeout 180s

Note: Current impl requires streaming data through the function, so timeout may need to be tweaked to ensure success.

## Test

### Store a file (README.md)

    curl --data-binary "@README.md" -H "Content-Type: application/octet-stream" \
    -H "x-bootstrap-id: test" -H "x-bootstrap-secret: my-secret" \
    "https://$REGION-$PROJECT_ID.cloudfunctions.net/bootstrapBackup"

Notes:
    * `x-bootstrap-id` holds the identifier of the file you're storing
    * `x-bootstrap-secret` holds the secret you'll pass to retrieve the file
    
Both values are required to retrieve. See Download below.

### Retrieve the file (as test.out)

    curl -o "test.out" -H "Accept: application/octet-stream" \
    -H "x-bootstrap-id: test" -H "x-bootstrap-secret: my-secret" \
    "https://$REGION-$PROJECT_ID.cloudfunctions.net/bootstrapRestore"

The files should be the same. If so, this output will be empty: 

    diff README.md test.out
