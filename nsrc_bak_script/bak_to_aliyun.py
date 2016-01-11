#!/usr/bin/python
# -*- coding: utf-8 -*-
import math
import os
import smtplib
import subprocess
import time
from email.mime.text import MIMEText
from tarfile import TarFile

import oss2
from oas.ease.vault import Vault
from oas.oas_api import OASAPI

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
# 日志文件
__log_file__ = os.path.join(CURR_DIR, 'nsrc_bak_script.log')

# 阿里云的一些配置信息
__aliyun__ = {
    'access_key': '<access_ke>',
    'access_secret': '<access_secret>',
    'oss_endpoint': 'http://oss-cn-beijing.aliyuncs.com',
    'oss_bucket': '<oss_bucket>',
    'oas_server': 'cn-hangzhou.oas.aliyuncs.com',
    'oas_vault': '<oas_vault>'
}

# 邮件SMTP信息
__email_smtp__ = {
    'server': "smtp.163.com",
    'port': 25,
    'username': '<username>',
    'password': '<password>'
}
# 备份信息 [ key:打包后的基础文件名， value：需要备份的文件/目录数组 ]

__backup__ = {
    'nsrc_web': ["/usr/local/apache2/htdocs/", "/usr/local/apache2/conf/"],
    'nsrc_database': ["/usr/local/mysql/data/", "/etc/my.cnf"]
}

# 备份文件加密密码
__archive_password__ = "<password>"


def write_log(content):
    with open(__log_file__, 'a') as log_file:
        content = "{curr_time}  : {content}\n".format(curr_time=time.strftime('%Y-%m-%d %H:%M:%S'), content=content)
        log_file.write(content)
        print(content)


def tar_gz_file(name, files):
    """
    打包并且压缩(gz)文件
    :param name: 打包文件名（不包含路径则打包当前脚本目录下）
    :param files: 需要打包文件的全路径名所组成的数组
    :return:
    """
    start_time = time.time()
    tarfile = TarFile.open(name, "w:gz")
    if isinstance(files, list):
        for f in files:
            tarfile.add(f)
    tarfile.close()
    use_time = math.floor(time.time() - start_time)

    message = "tar and gz file [{file_name}], use seconds:  {second}s".format(file_name=name, second=use_time)
    write_log(message)


def zip_file(name, files, password=None):
    """
    用zip打包并加密文件 （python的zipfile模块不能对创建的zip文件进行加密，这里采用7zip创建并加密zip文件）
    :param name: 打包文件名（不包含路径则打包当前脚本目录下）
    :param files: 需要打包文件的全路径名所组成的数组
    :param password: 加密密码
    :return:
    """
    start_time = time.time()
    os_command = ['7z', 'a', '-y']
    if password:
        os_command.append("-p{password}".format(password=password))
    os_command.append(name)

    if isinstance(files, list):
        for f in files:
            os_command.append(f)

    rc = subprocess.call(os_command)
    use_time = math.floor(time.time() - start_time)
    message = "zip file [{file_name}], use seconds:  {second}s".format(file_name=name, second=use_time)
    write_log(message)


def upload_to_aliyun_oss(file_path):
    """
    上传文件到阿里云对象存储服务器(oss)
    :param file_path: 文件路径名
    :return:
    """
    start_time = time.time()
    auth = oss2.Auth(__aliyun__.get('access_key'), __aliyun__.get('access_secret'))
    bucket = oss2.Bucket(auth, __aliyun__.get('oss_endpoint'), __aliyun__.get('oss_bucket'))
    with open(file_path, 'rb') as f:
        bucket.put_object(os.path.basename(f.name), f)

    use_time = math.floor(time.time() - start_time)
    message = "upload file to aliyun oss [{file_name}], use seconds:  {second}s".format(file_name=file_path, second=use_time)
    write_log(message)


def upload_to_aliyun_oas(file_path, desc=None):
    """
    上传文件到阿里云归档存储服务器(oss)
    :param file_path: 文件路径名
    :param desc:  归档文件的描述
    :return:  文件归档id
    """
    start_time = time.time()
    desc = desc if desc else os.path.basename(file_path)
    api = OASAPI(__aliyun__.get('oas_server'), __aliyun__.get('access_key'), __aliyun__.get('access_secret'))
    vault = Vault.create_vault(api, __aliyun__.get('oas_vault'))
    if not os.path.isfile(file_path):
        raise Exception("file is not exist [file]".format(file=file_path))
    size = os.path.getsize(file_path)
    if size < (200 * 1024 * 1024):
        archive_id = vault.upload_archive(file_path, desc)
    else:
        uploader = vault.initiate_uploader(file_path, desc)
        archive_id = uploader.start()

    use_time = math.floor(time.time() - start_time)
    message = "upload file to aliyun oas [{file_name}], use seconds:  {second}s".format(file_name=file_path, second=use_time)
    write_log(message)

    return archive_id


def send_email(subject, content, to_addrs):
    """
    邮件发送
    :param subject: 主题
    :param content: 内容
    :param to_addrs: 收件人列表
    :return:
    """
    start_time = time.time()
    if not isinstance(to_addrs, list):
        to_addrs = [].append(to_addrs)

    msg = MIMEText(content, 'html', 'utf-8')
    msg['From'] = __email_smtp__.get('username')
    msg['To'] = ' , '.join(to_addrs)
    msg['Subject'] = subject

    server = smtplib.SMTP(__email_smtp__.get('server'), __email_smtp__.get('port'))
    server.set_debuglevel(1)
    server.login(__email_smtp__.get('username'), __email_smtp__.get('password'))
    server.sendmail(__email_smtp__.get('username'), to_addrs, msg.as_string())
    server.quit()

    use_time = math.floor(time.time() - start_time)
    message = "end email to {to_addrs}, use seconds:  {second}s".format(to_addrs=to_addrs, second=use_time)
    write_log(message)


if __name__ == "__main__":
    write_log("=========================== start : backup data ==========================")

    bak_time = time.strftime('%Y%m%d%H%M')
    data_dir = os.path.join(CURR_DIR, 'data_dir')
    temp_dir = os.path.join(data_dir, 'temp')
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    archive_file_list = []
    # 1，打包备份文件
    for archive_name, files in __backup__.items():
        archive_name = os.path.join(temp_dir, '{base_name}_{bak_time}.tar.gz'.format(base_name=archive_name, bak_time=bak_time))
        tar_gz_file(archive_name, files)
        archive_file_list.append(archive_name)

    # 2，打包并加密所有的已打包的备份文件
    nsrc_bak_file = os.path.join(data_dir, 'nsrc_bak_{bak_time}.zip'.format(bak_time=bak_time))
    zip_file(nsrc_bak_file, archive_file_list, password=__archive_password__)
    # 3，删除中间的打包文件
    for archive_file in archive_file_list:
        os.remove(archive_file)

    # 4、上传到阿里云对象存储（oss）服务器。
    # 备注：当前阿里云的归档存储服务没有提供归档文件相应的查看和下载界面，所以这里用oss来存储备份文件，
    # 而且oss具有很好的生命周期规则可以设置，从而不需要自己来控制数据的过期删除。
    upload_to_aliyun_oss(nsrc_bak_file)

    # 5、对备份状态进行邮件通知
    subject = u'NSRC网站数据备份'
    content = u'''
    <h4>服务器[<ip>]上网站数据已经备份到阿里云对象存储（OSS）服务器上面</h4>
    <p><b>备份日期</b>：     {backup_time}</p>
    <p><b>备份文件名</b>：   {bak_file}</p>
    '''.format(backup_time=time.strftime('%Y-%m-%d %H:%M'), bak_file=os.path.basename(nsrc_bak_file))

    try:
        '''获取硬盘信息'''
        process = subprocess.Popen(['df', '-h'], stdout=subprocess.PIPE)
        out, err = process.communicate()
        content = content + u"<div style='padding:10px;background-color:#eee'><p>硬盘信息</p><hr/><p><pre>{disk_info}</pre></p></div>".format(
                disk_info=out)
    except Exception:
        pass
    send_email(subject, content, ['wuzhihui1123@163.com'])

    write_log("=========================== end : backup data ==========================")
