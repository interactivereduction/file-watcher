import logging
import os
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
    archive_dir = os.environ.get("ARCHIVE_DIR", "/archive")
    memphis_host = os.environ.get("QUEUE_HOST", "memphis.memphis.svc.cluster.local")
    memphis_station = os.environ.get("EGRESS_QUEUE_NAME", "watched-files")
    file_watcher_sha = os.environ.get("FILE_WATCHER_SHA256", "")
    db_ip = os.environ.get("DB_IP", "localhost")
    archive_pvc_name = f"{name}-file-watcher-pvc"
    archive_pv_name = f"{name}-file-watcher-pv"
    deployment_spec = yaml.safe_load(
        f"""
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
                    image: ghcr.io/interactivereduction/filewatcher@sha256:{file_watcher_sha}
                    env:
                    - name: MEMPHIS_HOST
                      value: {memphis_host}
                    - name: MEMPHIS_STATION
                      value: {memphis_station}
                    - name: MEMPHIS_PRODUCER_NAME
                      value: {name}-filewatcher
                    - name: WATCH_DIR
                      value: {archive_dir}
                    - name: FILE_PREFIX
                      value: {spec.get("filePrefix", "MAR")}
                    - name: INSTRUMENT_FOLDER
                      value: {spec.get("instrumentFolder", "NDXMAR")}
                    - name: DB_IP
                      value: {db_ip}

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
                        mountPath: {archive_dir}
                  volumes:
                    - name: archive-mount
                      persistentVolumeClaim:
                        claimName: {archive_pvc_name}
                        readOnly: true
        """
    )
    pvc_spec = yaml.safe_load(
        f"""
            kind: PersistentVolumeClaim
            apiVersion: v1
            metadata:
              name: {archive_pvc_name}
            spec:
              accessModes:
                - ReadOnlyMany
              resources:
                requests:
                  storage: 1000Gi
              volumeName: {archive_pv_name}
              storageClassName: smb
          """
    )

    pv_spec = yaml.safe_load(
        f"""
            apiVersion: v1
            kind: PersistentVolume
            metadata:
              annotations:
                pv.kubernetes.io/provisioned-by: smb.csi.k8s.io
              name: {archive_pv_name}
            spec:
              capacity:
                storage: 1000Gi
              accessModes:
                - ReadOnlyMany
              persistentVolumeReclaimPolicy: Retain
              storageClassName: smb
              mountOptions:
                - noserverino
                - _netdev
                - vers=2.1
                - uid=1001
                - gid=1001
                - dir_mode=0555
                - file_mode=0444
              csi:
                driver: smb.csi.k8s.io
                readOnly: true
                volumeHandle: archive.ir.svc.cluster.local/share##archive
                volumeAttributes:
                  source: "//isisdatar55.isis.cclrc.ac.uk/inst$/"
                nodeStageSecretRef:
                  name: archive-creds
                  namespace: ir
          """
    )

    return deployment_spec, pvc_spec, pv_spec


def deploy_deployment(deployment_spec, name, children):
    app_api = kubernetes.client.AppsV1Api()
    logger.info(f"Starting deployment of: {name} filewatcher")
    depl = app_api.create_namespaced_deployment(namespace="ir-file-watcher", body=deployment_spec)
    children.append(depl.metadata.uid)
    logger.info(f"Deployed: {name} filewatcher")


def deploy_pvc(pvc_spec, name, children):
    core_api = kubernetes.client.CoreV1Api()
    # Check if PVC exists else deploy a new one:
    if (
        pvc_spec["metadata"]["name"]
        not in core_api.list_namespaced_persistent_volume_claim(pvc_spec["metadata"]["namespace"]).items
    ):
        logger.info(f"Starting deployment of PVC: {name} filewatcher")
        pvc = core_api.create_namespaced_persistent_volume_claim(namespace="ir-file-watcher", body=pvc_spec)
        children.append(pvc.metadata.uid)
        logger.info(f"Deployed PVC: {name} filewatcher")


def deploy_pv(pv_spec, name, children):
    core_api = kubernetes.client.CoreV1Api()
    # Check if PV exists else deploy a new one
    if pv_spec["metadata"]["name"] not in core_api.list_persistent_volume().items:
        logger.info(f"Starting deployment of PV: {name} filewatcher")
        pv = core_api.create_persistent_volume(body=pv_spec)
        children.append(pv.metadata.uid)
        logger.info(f"Deployed PV: {name} filewatcher")


@kopf.on.create("ir.com", "v1", "filewatchers")
def create_fn(spec, **kwargs):
    name = kwargs["body"]["metadata"]["name"]
    logger.info(f"Name is {name}")

    deployment_spec, pvc_spec, pv_spec = generate_deployment_body(spec, name)
    # Make the deployment the child of this operator
    kopf.adopt(deployment_spec)
    kopf.adopt(pvc_spec)

    children = []
    deploy_pv(pv_spec, name, children)
    deploy_pvc(pvc_spec, name, children)
    deploy_deployment(deployment_spec, name, children)
    # Update controller's status with child deployment
    return {"children": children}
