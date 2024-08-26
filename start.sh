#!/bin/bash
 
cd /home/nandehutu/pve_backup
# 激活名为.venv的虚拟环境
source /home/nandehutu/pve_backup/.venv/bin/activate

# 判断虚拟环境是否激活成功
if [ $? -eq 0 ]; then
    echo "虚拟环境已成功激活"
else
    echo "虚拟环境激活失败"
    exit 1
fi
 
# 运行python程序
python /home/nandehutu/pve_backup/pve_backup.py
 
# 判断python程序是否运行成功
# if [ $? -eq 0 ]; then
#    echo "Python程序已成功运行"
# else
#     echo "Python程序运行失败"
#     exit 1
# fi
 
# 当所有任务完成后，关闭虚拟环境
# deactivate