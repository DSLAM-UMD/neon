#!/bin/bash
set -eux

PG_VERSION=${PG_VERSION:-14}

SPEC_FILE_ORIGINAL=/var/db/postgres/specs/spec-multi-region.json
SPEC_FILE=/tmp/spec.json

echo "Waiting pageserver become ready."
while ! nc -z ${PAGESERVER_HOST} 6400; do
     sleep 1;
done
echo "Page server is ready."

tenant_id=$(curl -s http://${PAGESERVER_HOST}:9898/v1/tenant/ | jq -r '.[0].id')
if [ $tenant_id = "null" ]; then
     echo "ERROR: Tenant id is null"
     exit 1
fi

timelines=$(curl -s http://${PAGESERVER_HOST}:9898/v1/tenant/${tenant_id}/timeline/)
region_id_to_timeline_id=$(echo ${timelines} | jq '[.[] | {key: .region_id, value: .timeline_id}] | from_entries')

timeline_id=$(echo ${region_id_to_timeline_id} | jq -r ".[\"${REGION}\"]")
if [ $timeline_id = "null" ]; then
     echo "ERROR: Timeline id is null"
     exit 1
fi

echo "Region id: ${REGION}"
echo "Tenant id: ${tenant_id}"
echo "Timeline id: ${timeline_id}"

echo "Overwrite variables in spec file"
sed "s/TENANT_ID/${tenant_id}/" ${SPEC_FILE_ORIGINAL} > ${SPEC_FILE}
sed -i "s/TIMELINE_ID/${timeline_id}/" ${SPEC_FILE}
sed -i "s/SAFEKEEPERS_ADDR/${SAFEKEEPERS_ADDR}/" ${SPEC_FILE}
sed -i "s/PAGESERVER_HOST/${PAGESERVER_HOST}/" ${SPEC_FILE}
sed -i "s/XACTSERVER/${XACTSERVER}/" ${SPEC_FILE}
sed -i "s/REGION/${REGION}/" ${SPEC_FILE}

cat ${SPEC_FILE}

echo "Start compute node"
/usr/local/bin/compute_ctl --pgdata /var/db/postgres/compute                    \
     -C "postgresql://cloud_admin@localhost:55433/postgres"                     \
     -b /usr/local/bin/postgres                                                 \
     -S ${SPEC_FILE}
     # --valgrind "valgrind --leak-check=no                                       \
     #                      --error-markers=VALGRINDERROR-BEGIN,VALGRINDERROR-END \
     #                      --suppressions=/shell/valgrind.supp                   \
     #                      --trace-children=yes                                  \
     #                      --time-stamp=yes"                                     
