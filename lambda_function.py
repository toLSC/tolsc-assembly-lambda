import boto3
import json
import io
import os
import shlex
import subprocess
import asyncio
import boto3.session
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import glob

client = boto3.client('s3')
LOCAL_DOWNLOAD_PATH ="/tmp/"
bucket_name =  "tolsc"

def download_object(s3_client, file_name):
    download_path = Path(LOCAL_DOWNLOAD_PATH) / file_name.split("/")[1].replace(" ","_")
    print(f"Descargando {file_name} hacia {download_path}")
    s3_client.download_file(
        bucket_name,
        file_name, 
        download_path
    )
    
def download_parallel_multithreading(keys_to_download):
    # Crear la sesion para obtener los archivos
    session = boto3.session.Session()
    s3_client = session.client("s3")

    # Realizar las tareas con el cliente S3
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_key = {executor.submit(download_object, s3_client, key): key for key in keys_to_download}
        for future in futures.as_completed(future_to_key):
            key = future_to_key[future]
            exception = future.exception()
            if not exception:
                yield key, future.result()
            else:
                yield key, exception
                
def get_length(input_video):
    result = subprocess.run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', input_video], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return float(result.stdout)

    
def lambda_handler(event, context):
    
    title = event['title']
    download = event['download']
    files_names=[]
    for i in download:
        files_names.append(i.split("/")[1])
    
    os.chdir('/tmp')
    # Descargar los archivos especificados
    for key, result in download_parallel_multithreading(download):
        print(f"{key} resultado: {result}")
    
    
    '''
    Escribir en un archivo el nombre de los archivos descargados para poderlos unir mas adelante.
    Asimismo se realiza un procesamiento sobre los videos descargados el cual permite obtener transiciones
    mas suaves entre videos
    '''
    
    with open('concat.txt', 'w+') as concat:
        for i in range(0,len(files_names)):
            files_names[i] = files_names[i].replace(" ","_")
            
            dur = get_length(f'{files_names[i]}')
            dur = round(dur, 2) - 0.8
            if i==0:
                cadena = f'/opt/bin/ffmpeg -to 00:00:0{dur} -y -i {files_names[i]} -c copy /tmp/output{i}.mp4'
            elif i==len(files_names)-1:
                cadena = f'/opt/bin/ffmpeg -ss 00:00:00.5 -y -i {files_names[i]} -c copy /tmp/output{i}.mp4'
            else:
                cadena = f'/opt/bin/ffmpeg -ss 00:00:00.5 -to 00:00:0{dur} -y -i {files_names[i]} -c copy /tmp/output{i}.mp4'
            comand = shlex.split(cadena)
            subprocess.call(comand)
            concat.write(f'file output{i}.mp4\n')

        
    '''
    Con el archivo de los nombres de los videos a descargar se utiliza el siguiente comando 
    el cual une los videos en un archivo llamado out.mp4  
    '''
    ffmpeg_cmd = '/opt/bin/ffmpeg -f concat -safe 0 -y -i /tmp/concat.txt -c copy /tmp/out.mp4'
    command = shlex.split(ffmpeg_cmd)
    subprocess.call(command)
    
    response = client.upload_file('/tmp/out.mp4', bucket_name, 'videos/'+title+'.mp4')
    try:
        response = client.upload_file('/tmp/out.mp4', bucket_name, 'videos/'+title+'.mp4')
        return True
    except:
        return False
    