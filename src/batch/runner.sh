export BATCH_ID=$1
export DATA_SOURCE=$2

# Mount point for the relevant Google Cloud bucket.
export MOUNT_ROOT=/mnt/share

# Create directory for logs, if necessary.
export LOG_DIR=${MOUNT_ROOT}/logs/${BATCH_ID}
mkdir -p ${LOG_DIR}

# Redirect all subsequent logs.
# The $BATCH_TASK_INDEX variable is set by Google Batch.
exec &> ${LOG_DIR}/${BATCH_TASK_INDEX}.log

echo ">>> Update APT"
sudo apt -y update

echo ">>> Install packages"
sudo apt -y install python3 python3-pip python3-setuptools

echo ">>> Update PIP and setuptools"
sudo python3 -m pip install --upgrade pip setuptools
echo $?

echo ">>> Install packages"
sudo python3 -m pip install -r ${MOUNT_ROOT}/code/requirements.txt
echo $?

echo ">>> Run script"
sudo python3 ${MOUNT_ROOT}/code/ingest.py ${DATA_SOURCE} ${BATCH_TASK_INDEX}
export RETURNCODE=$?

echo ">>> Completed with return code: ${RETURNCODE}"
exit ${RETURNCODE}
