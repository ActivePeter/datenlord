# Prepare

readonly NAMESPACE="csi-datenlord"
readonly ASYNC_FUSE_APP="datenlord-async-fuse"
readonly TEST_DIR="/var/opt/datenlord-data"


echo "1.Remove previous datenlord and apply s3 datenlord"
echo "》》》"
kubectl delete -f scripts/datenlord_s3_test/datenlord-s3.yaml
sleep 10
kubectl apply -f scripts/datenlord_s3_test/datenlord-s3.yaml
sleep 60
kubectl wait --for=condition=Ready pod -l app=datenlord-async-fuse -n csi-datenlord --timeout=120s

# Persist
echo "2. Create a dir and a file in mount path"
echo "》》》"
kubectl get pods -A
POD1=$(kubectl get pods -A | grep "datenlord-async" | awk 'NR==1{print $2}')
kubectl exec -i -t ${POD1} -n csi-datenlord -- mkdir ${TEST_DIR}/d1
kubectl exec -i -t ${POD1} -n csi-datenlord -- touch ${TEST_DIR}/f1
ls_res=$(kubectl exec -i -t ${POD1} -n csi-datenlord -- ls ${TEST_DIR})

echo "3. Reload async fuse and check whether the metadata is persisted."
echo "》》》"
kubectl delete -f scripts/datenlord_s3_test/datenlord-s3.yaml
sleep 10
kubectl apply -f scripts/datenlord_s3_test/datenlord-s3.yaml
sleep 60
kubectl wait --for=condition=Ready pod -l app=datenlord-async-fuse -n csi-datenlord --timeout=120s

kubectl get pods -A
POD1=$(kubectl get pods -A | grep "datenlord-async" | awk 'NR==1{print $2}')
ls_res2=$(kubectl exec -i -t ${POD1} -n csi-datenlord -- ls ${TEST_DIR})

if [ $ls_res -ne $ls_res2 ]; then echo "Metadata persist has error"; exit 1; fi
echo "Metadata persist is ok"
