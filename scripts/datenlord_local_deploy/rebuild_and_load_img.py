# run in prj root dir
import subprocess
def runcmd(cmd,exit_when_fail=True):
    ret=subprocess.run(cmd.split(" "))
    if ret.returncode==0:
        print(">>> ",cmd," succ \n")
    else:
        print(">>> ",cmd," fail ",ret,"\n")
        if exit_when_fail:
            exit()


runcmd("docker build . "+
            "--build-arg RUST_IMAGE_VERSION=1.67.1 "+
            "--file ./scripts/datenlord_local_deploy/Dockerfile "+
            "--target datenlord "+
            "--tag datenlord/datenlord:e2e_test" )
runcmd("kind load docker-image datenlord/datenlord:e2e_test")
# sh ./temp.sh