import yaml

# 读取Yaml文件方法
def read_yaml(yaml_path):
    with open(yaml_path, encoding="utf-8", mode="r") as f:
        result = yaml.load(stream=f,Loader=yaml.FullLoader)
        return result

content=read_yaml(".github/workflows/cron.yml")
envs=content["env"]

script_content=""
for key in envs:
    script_content+="export {}={}\necho {}\n".format(key,envs[key],"${"+key+"}")

fileName='scripts/export_env_var_for_sh.sh'
with open(fileName,'w',encoding='utf-8') as file:
    file.write(script_content)
