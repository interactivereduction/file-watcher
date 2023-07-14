import logging
import sys

import kopf as kopf
import kubernetes
import yaml

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def generate_deployment_body(spec, name):
    deployment_spec = yaml.safe_load(f"""
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: {name}-file-watcher-deployment
              labels:
                app: {name}-file-watcher
            spec:
              replicas: 1
              selector:
                matchLabels:
                  app: {name}-file-watcher
              template:
                metadata:
                  labels:
                    app: {name}-file-watcher
                spec:
                  containers:
                  - name: {name}-file-watcher
                    image: ghcr.io/interactivereduction/filewatcher@sha256:{spec.get("fileWatcherSha256", "")}
                    env:
                    - name: MEMPHIS_HOST
                      value: {spec.get("memphisHost", "localhost")}
                    - name: MEMPHIS_STATION
                      value: {spec.get("memphisStation", "watched-files")}
                    - name: MEMPHIS_PRODUCER_NAME
                      value: {name}-filewatcher
                    - name: WATCH_DIR
                      value: {spec.get("archiveDir", "/archive")}
                    - name: FILE_PREFIX
                      value: {spec.get("filePrefix", "MAR")}
                    - name: INSTRUMENT_FOLDER
                      value: {spec.get("instrumentFolder", "NDXMAR")}
                    - name: DB_IP
                      value: {spec.get("dbIp", "localhost")}

                    # Secrets
                    - name: MEMPHIS_USER
                      valueFrom: 
                        secretKeyRef:
                          name: filewatcher-secrets
                          key: memphis_user
                    - name: MEMPHIS_PASS
                      valueFrom: 
                        secretKeyRef:
                          name: filewatcher-secrets
                          key: memphis_password
                    - name: DB_USERNAME
                      valueFrom:
                        secretKeyRef:
                          name: filewatcher-secrets
                          key: db_username
                    - name: DB_PASSWORD
                      valueFrom:
                        secretKeyRef:
                          name: filewatcher-secrets
                          key: db_password
                    volumeMounts:
                      - name: archive-mount
                        mountPath: {spec.get("archiveDir", "/archive")}
                  volumes:
                    - name: archive-mount
                      hostPath:
                        type: Directory
                        path: {spec.get("archiveDir", "/archive")}     
        """)
    return deployment_spec


@kopf.on.create("ir.com", "v1", "filewatchers")
def create_fn(spec, **kwargs):
    name = kwargs["body"]["metadata"]["name"]
    logger.info(f"Name is {name}")

    deployment_spec = generate_deployment_body(spec, name)
    # Make the deployment the child of this operator
    kopf.adopt(deployment_spec)

    # Create the object by using the kubernetes api
    api = kubernetes.client.AppsV1Api()
    try:
        logger.info(f"Starting deployment of: {name} filewatcher")
        depl = api.create_namespaced_deployment(namespace=deployment_spec['metadata']['namespace'],
                                                body=deployment_spec)
        logger.info(f"Deployed: {depl}")
        # Update controller's status with child deployment
        return {"children": [depl.metadata.uid]}
    except Exception as e:
        logger.error("Exception raised when creating deployment: %s", e)
