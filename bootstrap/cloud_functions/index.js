const gcs = require('@google-cloud/storage')()
const contentType = 'application/octet-stream'
const backupIDHeader = 'x-bootstrap-id'
const secretHeader = 'x-bootstrap-secret'

// default values...
var bucketName = 'transicator-bootstrap-data'
var minBackupAge = 60*60*1000 // 60 minutes

try {
    let config = require("./config.json")
    console.log("config.json found, using contained values")
    if (config.bucketName) bucketName = config.bucketName
    if (config.minBackupAge) minBackupAge = config.minBackupAge
} catch (err) {
    console.log("no config.json found, using defaults")
}

exports.bootstrapBackup = (req, res) => {

    if (req.method !== 'POST') {
        return res.status(405).send('method must be POST')
    }

    if (req.get('content-type') !== contentType) {
        return res.status(406).send('content-type must be ' + contentType)
    }

    let id = req.get(backupIDHeader)
    if (!id) {
        return res.status(400).send('id is required')
    }

    let secret = req.get(secretHeader)
    if (!secret) {
        return res.status(400).send('secret is required')
    }

    let bucket = gcs.bucket(bucketName)

    bucket.exists((err, exists) => {
        if (err) {
            let s = err.toString() + " (1)"
            console.error(s)
            return res.status(500).send(s)
        }

        if (!exists) {
            return res.status(500).send('bucket ' + bucketName + " doesn't exist")
        }

        let file = bucket.file(id)

        // check if file exists, and the file age, reject if younger than minBackupAge
        file.exists((err, exists) => {
            if (err) {
                let s = err.toString() + " (2)"
                console.error(s)
                return res.status(500).send(s)
            }

            if (exists) {

                let backupCreated = Date.parse(file.metadata.timeCreated)
                let backupAge = Date.now() - backupCreated

                if (backupAge < minBackupAge) {
                    return res.status(429).send('no need for backup right now')
                }
            }

            let writeOpts = {
                metadata: {
                    contentType: contentType,
                    metadata: {
                        secret: secret,
                    },
                },
            }

            let writer = file.createWriteStream(writeOpts)
            writer.write(req.body)
            writer.end()

            res.send(`Thank you, ${id}.`)
        })
    })
}

exports.bootstrapRestore = (req, res) => {

    if (req.method !== 'GET') {
        return res.status(405).send('method must be GET')
    }

    if (req.get('accept') !== contentType) {
        return res.status(400).send('accept must be ' + contentType)
    }

    let id = req.get(backupIDHeader)
    if (!id) {
        return res.status(400).send('id is required')
    }

    let secret = req.get(secretHeader)
    if (!secret) {
        return res.status(401).send('unauthorized')
    }

    let bucket = gcs.bucket(bucketName)

    bucket.exists((err, exists) => {
        if (err) {
            let s = err.toString() + " (1)"
            console.error(s)
            return res.status(500).send(s)
        }

        if (!exists) {
            return res.status(500).send('bucket ' + bucketName + " doesn't exist")
        }

        let file = bucket.file(id)
        file.exists((err, exists) => {
            if (err) {
                let s = err.toString() + " (2)"
                console.error(s)
                return res.status(500).send(s)
            }

            if (!exists) {
                return res.status(404).send('file ' + id + " doesn't exist")
            }

            file.getMetadata((err, metadata) => {
                if (err) {
                    let s = err.toString() + " (3)"
                    console.error(s)
                    return res.status(500).send(s)
                }

                if (metadata.metadata.secret !== secret) {
                    return res.status(403).send("not allowed")
                }

                res.setHeader('Content-Type', contentType)

                file
                    .createReadStream()
                    .on('error', function(err) {
                        let s = err.toString() + " (4)"
                        console.error(s)
                        return res.status(500).send(s)
                    })
                    .pipe(res)
            })
        })
    })
}

// sadly, signed urls don't currently work (see: https://github.com/GoogleCloudPlatform/gcloud-common/issues/180)
// so for now, we can't include this code to avoid streaming the backup data through the script
// let config = {
//     action: 'read',
//     expires: Date.now() + (15*60*1000) // 15 minutes
// }
// file.getSignedUrl(config, (err, url) => {
//     if (err) {
//         return res.status(500).send(err.toString())
//     }
//
//     res.writeHead(302, {
//         'Location': url
//     })
//     res.end()
// })

