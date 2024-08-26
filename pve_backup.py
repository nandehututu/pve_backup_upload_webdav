import math
import os
import queue
import re
import shutil
import threading
import time
import logging
import paramiko
import datetime
from datetime import datetime
from datetime import timedelta
from paramiko.ssh_exception import SSHException
from scp import SCPClient
from tqdm import tqdm
import subprocess
import psutil
from webdav3.client import Client

# PVE 相关参数
PVE_IP = 'ip'  # PVE IP地址
PVE_PORT = 32002  # PVE SSH端口
PVE_USERNAME = 'USERNAME'  # PVE 用户名
PVE_PASSWORD = 'PASSWORD'  # PVE 密码
PVE_DEFAULT_VM_IDS = [800,801]  # 虚拟机默认编号列表
# PVE_VM_IDS = [104,100,130,201,202,204,214,215,216,217,800,801]  # 虚拟机编号列表
PVE_WAIT_TIME = 30 # 筛选VMIDS输入等待时间，单位秒
PVE_REMOTE_DIR = '/mnt/pve/pve-sdb-file/dump'  # 远程文件夹绝对路径
PVE_FILENAME_SUFFIXES = ('.vma', '.vma.lzo', '.vma.gz',  '.vma.zst')   # 文件后缀，分别对应pve的备份中的4种压缩格式

# WebDAV 1相关参数
WEBDAV_URL = 'https://ex.aaaa.cn/dav/'  # WebDAV服务器URL
WEBDAV_PASSWORD = 'PASSWORD'  # WebDAV 密码
WEBDAV_USERNAME_1 = 'WEBDAV_USERNAME_1'  # WebDAV 用户名
WEBDAV_USERNAME_2 = 'pve_backup_alist_baidu'  # WebDAV 用户名
WEBDAV_MAX_RETRIES = 9  # WebDAV最大重试次数
WEBDAV_RETRY_DELAY = 5  # WebDAV重试间隔，单位秒

# log相关参数
LOG_DIR = 'logs'  # log文件路径(python路径)
LOG_LEVEL = 'logging.DEBUG' # 日志级别，包含ERROR、WARNING、INFO、DEBUG、TRACE

# 进度条相关参数
ALIVE_PROGRESS_INTERVAL = 0.5  # 进度条刷新间隔，单位秒
ALIVE_BAR_LENGTH = 50  # 进度条长度

# 压缩相关参数
Z_LOCAL_TEMP_DIR = 'tmp'  # 本地临时文件夹(python路径)
Z_VOLUME_SIZE_MB = 95  # 卷大小，单位兆
Z_ENCRYPTION_KEY = 'PASSWORD'  # 加密密钥
Z_COMPRESSION_LEVEL = 0 # 设置压缩等级 0-9 0为最低，9为最高



# 初始化日志
def setup_logging(LOG_LEVEL=logging.INFO):
    # 创建日志文件夹
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        
    # 设置日志文件名
    log_filename = os.path.join(LOG_DIR, datetime.now().strftime("%Y%m%d.log"))
    
    # 创建一个logger
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)

    # 创建一个handler用于写入日志文件
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(LOG_LEVEL)

    # 创建一个handler用于输出到控制台
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LOG_LEVEL)

    # 定义handler的输出格式
    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # 给logger添加handler
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    # logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# 初始化SSH连接
def ssh_connect(host, port, username, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    client.connect(host, port=port, username=username, password=password)
    return client

# 获取远端文件列表
def get_remote_files(client, directory):
    stdin, stdout, stderr = client.exec_command(f'ls -l {directory} | grep ".vma.zst"')
    files = []
    for line in stdout.readlines():
        parts = line.split()
        filename = parts[-1]
        size = int(parts[4])
        if filename.endswith(PVE_FILENAME_SUFFIXES):
            files.append((filename, size))
    return files

# 获取最新备份文件
def get_latest_backup(files, vm_id):
    latest_file = None
    latest_time = datetime(1970, 1, 1, 0, 0, 0)
    for filename, _ in files:
        match = re.match(r'vzdump-qemu-(\d+)-(\d{4}_\d{2}_\d{2}-\d{2}_\d{2}_\d{2})\.vma\.zst', filename)
        if match and int(match.group(1)) == vm_id:
            file_time = datetime.strptime(match.group(2), '%Y_%m_%d-%H_%M_%S')
            if file_time > latest_time:
                latest_time = file_time
                latest_file = filename
    return latest_file

# 下载文件
def download_file(client, remote_path, local_path):
    try:
        with SCPClient(client.get_transport(), progress=progress) as scp:
            scp.get(remote_path, local_path)
            print("\n")
            logging.info(f"下载 {remote_path} 完成 ")
        return True
    except Exception as e:
        print("\n")
        logging.error(f"下载 {remote_path} 出错: {e}")
        return False
    
# 验证文件大小
def verify_file_size(filename, expected_size):
    actual_size = os.path.getsize(filename)
    return actual_size == expected_size

# 改变指定文件夹及其内部所有文件和子文件夹的权限为755。
def change_permissions(folder_path):
    chmod_command = ['chmod', '-R', '755', folder_path]
    subprocess.run(chmod_command, check=True)
# 进度条
def progress(filename, size, sent):
    global last_progress_time
    percent = int(sent / size * 100)
    filled_length = int(ALIVE_BAR_LENGTH * percent / 100)
    if time.time() - last_progress_time >= ALIVE_PROGRESS_INTERVAL:
        bar = '█' * filled_length + '-' * (ALIVE_BAR_LENGTH - filled_length)
        animation = ['|', '/', '-', '\\']
        print(f"\r{filename}: [{bar}] {int(percent):3}% {animation[percent % len(animation)]}", end='')

# 压缩并加密文件
def compress_and_encrypt_7z(src, dst, key, volume_size=None, iter_count=None, use_pbkdf2=False):
    # 先改变文件夹权限
    change_permissions(Z_LOCAL_TEMP_DIR)
    
    # 构建 7z 命令
    cmd_7z = [
        '7z',
        'a',  # 添加文件到存档
        f'-mx{Z_COMPRESSION_LEVEL}',  # 设置压缩等级   
        '-t7z',  # 使用 7z 格式
        f'-p{key}',  # 设置密码
        '-bt' , # 显示执行时间统计信息
    ]

    # 如果指定了分卷大小，则使用 7z 分卷压缩
    if volume_size is not None:
        # 构建分卷压缩命令
        cmd_7z.extend([
            '-v' + str(volume_size) + 'm',  # 设置分卷大小
        ])

    # 7z需要目标文件在前，压缩文件在后
    cmd_7z.extend([
        f'./{dst}',  # 输出文件名
        f'./{src}',  # 源文件路径
    ])
    
    # 执行 7z 命令
    logging.info(f"开始压缩 {src} 到 {dst}")
    subprocess.run(cmd_7z, check=True)

    # 删除原始文件
    try:
        os.remove(f'./{src}')
        logging.info(f"原文件 {src} 已被删除。")
    except Exception as e:
        logging.error(f"删除原文件失败: {e}")

    return True

# 上传到WebDAV
def upload_to_webdav(file_path, WEBDAV_URL, WEBDAV_USERNAME, WEBDAV_PASSWORD):
    # 获取文件名，示例：vzdump-qemu-800-2024_08_25-14_50_39.vma.zst
    filename = os.path.basename(f'./{file_path}') 
    vm_id, file_time, file_data= extract_info_from_filename(filename)
    # 这里文件夹名还是使用文件夹名字上的时间而非现在时间(datetime.now().strftime("%Y-%m-%d"))
    WEBDAV_ROOT = f'{file_data}/{vm_id}'
    NETWORK_DISK = WEBDAV_USERNAME.split('_')[3]
    webdav_options = {
        'webdav_hostname': WEBDAV_URL,  # WebDAV地址
        'webdav_login': WEBDAV_USERNAME, # 用户名
        'webdav_password': WEBDAV_PASSWORD, # 密码
        'webdav_root': WEBDAV_ROOT, # webdav目录
        'webdav_timeout': 360000, # 超时时间 ，单位s
        'disable_check': True, #有的网盘不支持check功能
    }
    
    # 创建 WebDAV 客户端实例
    client = Client(webdav_options) 
    
    # 使用 list 方法获取远程文件列表
    try:
        remote_files = client.list()
    except  Exception as e:
        # msg = "Error: {}.{}".format(type(e),  str(e))
        remote_files = []
        
    # 检查远程是否存在同名文件
    if filename in remote_files:
        logging.info(f"文件 {filename} 已存在，正在删除...")
        # 删除同名文件
        client.clean(filename)
    
    # 上传文件，remote_path为服务器上传文件名，  local_path为本地文件路径
    retries = 0
    while retries < WEBDAV_MAX_RETRIES:
        try:
            # 尝试上传文件
            logging.info(f"[{NETWORK_DISK}({retries+1})]正在上传: {filename}")
            client.upload(remote_path=filename, local_path=file_path)
            # 如果成功，则跳出循环
            break
        except Exception as e:
            # 如果上传失败，则打印错误信息并等待一段时间后重试
            logging.error(f"[{NETWORK_DISK}({retries+1})]上传失败: {filename} 重新上传中...\n{e}")
            time.sleep(WEBDAV_RETRY_DELAY)
            retries += 1
    else:
        # 如果所有重试都失败了，则抛出异常
        raise Exception("经过最大重试次数后，文件上传失败")
    logging.info(f"[{NETWORK_DISK}]上传成功: {filename}") 

# 清空临时文件夹
def delete_temp_path(directory):
    directory = f'./{directory}'
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
                logging.info(f"已成功删除文件: {file_path}")
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
                logging.info(f"已成功删除文件夹: {file_path}")
        except Exception as e:
            logging.error(f"删除{file_path}失败. Reason: {e}")

# 耗时时间换算
def format_duration(seconds):
    if seconds < 0:
        return "时间倒退了？"
    
    delta = timedelta(seconds=seconds)
    years, days = divmod(delta.days, 365)
    months, days = divmod(days, 30)  # 简化处理，假设每个月都是30天
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if years > 0:
        parts.append(f"{years}年")
    if months > 0:
        parts.append(f"{months}月")
    if days > 0:
        parts.append(f"{days}天")
    if hours > 0:
        parts.append(f"{hours}小时")
    if minutes > 0:
        parts.append(f"{minutes}分钟")
    parts.append(f"{seconds}秒")

    return ' '.join(parts)

# 从文件名提取信息
def extract_info_from_filename(filename):
    match = re.match(r'vzdump-qemu-(\d+)-(\d{4}_\d{2}_\d{2}-\d{2}_\d{2}_\d{2})\.vma\.zst', filename)
    if match:
        vm_id = int(match.group(1))
        file_time = datetime.strptime(match.group(2), '%Y_%m_%d-%H_%M_%S')
        file_data = datetime.strftime(file_time, '%Y_%m_%d')
        return vm_id, file_time, file_data

# 等待输入功能，WAIT_TIME后退出，并输入默认值
def get_input(timeout, default, ):
    # 创建一个事件对象
    event = threading.Event()

    # 创建一个队列用于线程间通信
    user_input_queue = queue.Queue()
    def input_thread():
        # 输入函数
        user_input = input(f"请在{timeout}秒内通过命令行输入虚拟机ID，用逗号或空格分隔，按回车确认: \n")
        # 将输入字符串分割成列表，并尝试转换为整数
        try:
            user_input_num = [int(num.strip()) for num in user_input.replace('，', ',').replace(' ', ',').split(',') if num.strip()]
        except ValueError:
            print("输入包含非数字字符，请重新输入或等待超时使用默认值")
            user_input_num = [None]
        # 将结果放入队列
        user_input_queue.put(user_input_num)
        # 设置事件标志为真
        event.set()
    # 启动输入线程
    thread = threading.Thread(target=input_thread)
    thread.start()
    # 等待输入线程或者超时结束
    thread.join(timeout)
    
    # 如果没有完成则使用默认值
    if not event.is_set():
        print("等待超时，使用默认值:", default)
        return list(default) if not isinstance(default, list) else default   
    else:
        return user_input_queue.get()
    
# 根据时间筛选id列表
def get_VM_IDS(vm_id):
    # 获取当前时间
    current_time = time.localtime()

    # 根据当前小时数筛选虚拟机ID
    if current_time.tm_hour == 4:
        PVE_VM_IDS = [800, 801]
    # elif current_time.hour == 4:
    #     PVE_VM_IDS = [vm_id for vm_id in PVE_VM_IDS if vm_id not in [800, 801]]
    else:
        # 等待用户通过命令行输入，或直接使用所有虚拟机ID
        PVE_VM_IDS = get_input(PVE_WAIT_TIME, PVE_DEFAULT_VM_IDS)        
    return PVE_VM_IDS

# 主函数
def main():
    setup_logging()
    logging.info("开始备份流程...")

    # 清空临时文件夹
    delete_temp_path(Z_LOCAL_TEMP_DIR)

    # 连接SSH
    client = ssh_connect(PVE_IP, PVE_PORT, PVE_USERNAME, PVE_PASSWORD)

    # 获取所有远程文件
    remote_files = get_remote_files(client, PVE_REMOTE_DIR)
    remote_files.sort(key=lambda x: x[1])  # 按文件大小排序

    # 打印详细信息
    global PVE_VM_IDS
    PVE_VM_IDS = []
    logging.info("\n虚拟机信息:\n虚拟机编号\t文件名称\t时间\t文件大小")
    for filename, size in remote_files:
        vm_id, file_time, file_data= extract_info_from_filename(filename)
        if vm_id not in PVE_VM_IDS:
            PVE_VM_IDS.append(vm_id)  # 将 vm_id 加入到 PVE_VM_IDS 列表中
        logging.info(f"{vm_id}\t{filename}\t{file_time}\t{size} 字节")

    PVE_VM_IDS = sorted(PVE_VM_IDS)
    logging.info(f"虚拟机数量: {len(PVE_VM_IDS)}")
    logging.info(f"虚拟机编号: {PVE_VM_IDS}")
    # 根据时间筛选PVE_VM_IDS
    selected_pvevmids = get_VM_IDS(PVE_VM_IDS)

    # 根据虚拟机编号list，由占用空间从小到大确定下载顺序
    sorted_vms = sorted(selected_pvevmids, key=lambda vm_id: next((size for filename, size in remote_files if re.match(r'vzdump-qemu-(\d+)-', filename).group(1) == str(vm_id)), float('inf')))
    

    for vm_id in sorted_vms:
        # 获取最新的备份文件
        latest_file = get_latest_backup(remote_files, vm_id)
        if latest_file is None:
            logging.warning(f"未找到虚拟机编号 {vm_id} 的备份")
            continue

        # 路径拼接汇总
        # pve系统中备份文件绝对路径 
        # 示例：/mnt/pve/pve-sdb-file/dump/vzdump-qemu-104-2024_08_23-03_29_56.vma.zst
        remote_path = f"{PVE_REMOTE_DIR}/{latest_file}"
        # 本地临时文件绝对路径 
        # 示例：/tmp/vzdump-qemu-104-2024_08_23-03_29_56.vma.zst
        local_path = os.path.join(Z_LOCAL_TEMP_DIR, latest_file)
        # 本地加密文件绝对路径  
        # 示例：/tmp/vzdump-qemu-104-2024_08_23-03_29_56.vma.zst.enc
        encrypted_path = os.path.join(Z_LOCAL_TEMP_DIR, f'{latest_file}.enc')

        # 下载文件
        start_time = time.time()
        global last_progress_time
        last_progress_time = time.time()
        while not download_file(client, remote_path, local_path):
            logging.warning(f"{latest_file} 下载失败，正在尝试重新下载...")
            time.sleep(5)  # Wait before retrying

        # 验证文件大小
        stdin, stdout, stderr = client.exec_command(f'du -b {remote_path}')
        remote_size = int(stdout.read().decode().split()[0])
        if not verify_file_size(local_path, remote_size):
            logging.error(f"{latest_file} 的文件大小验证失败")
            continue
        else:
            logging.info(f"{latest_file} 文件大小验证成功")

        # 压缩加密
        if not compress_and_encrypt_7z(local_path, encrypted_path, Z_ENCRYPTION_KEY, Z_VOLUME_SIZE_MB):
            logging.error(f"{latest_file} 压缩加密失败")
            continue
        else:
            logging.info(f"{latest_file} 压缩加密成功")

        # 遍历临时目录中的所有文件上传
        for filename in os.listdir(Z_LOCAL_TEMP_DIR):
            file_path = os.path.join(Z_LOCAL_TEMP_DIR, filename)
            if os.path.isfile(file_path):
                last_progress_time = time.time() 
                upload_to_webdav(file_path, WEBDAV_URL, WEBDAV_USERNAME_1, WEBDAV_PASSWORD)
                upload_to_webdav(file_path, WEBDAV_URL, WEBDAV_USERNAME_2, WEBDAV_PASSWORD)

        # 删除临时文件
        delete_temp_path(Z_LOCAL_TEMP_DIR)

        end_time = time.time()
        logging.info(f"{latest_file} 处理完成。耗时: {format_duration(end_time - start_time)}")

    # 关闭SSH连接
    client.close()

if __name__ == "__main__":
    main()
