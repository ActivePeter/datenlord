import random, os, time
tree={}

def generate_random_str(randomlength=16):
    random_str =''
    base_str ='ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789'
    length =len(base_str) -1
    for i in range(randomlength):
        random_str +=base_str[random.randint(0, length)]
    return random_str
def recur_create(parent,depth):
    if depth<0:
        return
    count=random.randint(3, 10)
    for i in range(count):
        fname=generate_random_str()
        parent[fname]={}
        if random.randint(0,1)==0: #dir
            recur_create(parent[fname],depth-1)
def recur_print(node,depth):
    for key in node:
        str1=""
        for i in range(0,depth):
            str1+=" "
        str1+=key
        print(str1)
        recur_print(node[key],depth+1)
    
def iter_tree(node,fullpath,cb):
    for key in node:
        cb(fullpath+"/"+key,len(node[key])==0)
        iter_tree(node[key],fullpath+"/"+key,cb)

import subprocess
def runcmd(cmd):
    ret=subprocess.run(cmd.split(" "))
    if ret.returncode==0:
        print(">>> ",cmd," succ \n")
    else:
        print(">>> ",cmd," fail ",ret,"\n")


# Create mem tree structure first
recur_create(tree,3)
# recur_print(tree,0)

prefix="testdir"

pathmap={}

runcmd("rm -rf "+prefix)
runcmd("mkdir "+prefix)

# Create in fs
def cb(fullpath,is_file):
    global pathmap
    # print(fullpath,is_file)
    if is_file:
        runcmd("touch "+prefix+fullpath)
    else:
        runcmd("mkdir "+prefix+fullpath)
    pathmap[prefix+fullpath]=1

iter_tree(tree,"",cb)

time.sleep(1)

# Check in fs
def show_files(base_path, all_files=[]):
    file_list = os.listdir(base_path)
    for file in file_list:
        cur_path = os.path.join(base_path, file)
        if os.path.isdir(cur_path):
            show_files(cur_path, all_files)
        all_files.append(cur_path)
    return all_files

allf=show_files(prefix)
print(allf)

#Check equality
fcount=0
for f in allf:
    assert f in pathmap
    fcount+=1
print("All file count:",len(pathmap),fcount)
assert len(pathmap)==fcount