# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 10:27:31 2019

@author: win10
"""
import linecache
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt               
import networkx as nx 
import json


def read_data(filename,line_num):
    chunks=pd.read_csv(filename,iterator = True,header=None)
    chunk=chunks.get_chunk(line_num)
    return chunk

#machine_meta = read_data('machine_meta.csv',300)
#machine_meta.columns = ['machine_id','time_stamp','disaster_level_1',
#                 'disaster_level_2','cpu_num','mem_size','status']
#
#
#machine_usage=read_data('machine_usage.csv',300)
#machine_usage.columns = ['machine_id','time_stamp','cpu_util_percent',
#                 'mem_util_percent','mem_gps','mpki','net_in','net_out',
#                 'disk_io_percent']
#
#
#container_meta=read_data('container_meta.csv',300)
#container_meta.columns = ['container_id','machine_id','time_stamp','deploy_unit',
#                          'status','cpu_request','cpu_limit','mem_size']
#
#
#container_usage=read_data('container_usage.csv',300)
#container_usage.columns = ['container_id','machine_id','time_stamp','cpu_util_percent',
#                          'mem_util_percent','cpi','mem_gps','mpki',
#                          'net_in','net_out','disk_io_percent']
#


batch_task=read_data('batch_task.csv',1000)
batch_task.columns = ['task_name','inst_num','job_name','task_type',
                          'status','start_time','end_time','plan_cpu',
                          'plan_mem']


#batch_instance=read_data('batch_instance.csv',300)
#batch_instance.columns = ['inst_name','task_name','job_name','task_type',
#                          'status','start_time','end_time','machine_id',
#                          'seq_no','total_seq_no','cpu_avg','cpu_max',
#                          'mem_avg','mem_max']
#

#======================统计文件行数===================================
#count = -1
#for count, line in enumerate(open('batch_task.csv', 'rU')):
#    pass
#count += 1

def calJobsInfo(batch_task_final,element_job_count):
    """
    输入为经数据清洗后的文件和文件块中出现的job种类，按照job_name对原始数据切片
    """
    jobs_info=[]
    for key in element_job_count:
        job_info=batch_task_final[batch_task_final['job_name']==key]
        jobs_info.append(job_info)
    return jobs_info

def dropOutlier(df):
    """
    删除task中名为mergetask的未知任务
    """
    a=[]
    for i,item in enumerate(df['task_name']):
        if item.isalpha():
            print(str(i)+':'+item)
            a.append(i)
    for i in reversed(a):
        df=df.drop(df.index.values[i])
    return df


def extractTask(jobs_info):
    job_task_list=[]
    for job in jobs_info:
        task=job['task_name'].tolist()
        job_task_list.append(task)
    return job_task_list


def find_maxvalue(line):
    r=[]
    maxvalue = 0
    for i in range(len(line)):
        a=int(line[i])
        r.append(a)
        if a > maxvalue:
            maxvalue = a
    return maxvalue,r


def str_to_int(task):
    d=[]
    maxvalue=[]
    for i in range(len(task)):
        b=task[i].split(task[i][0])[1].split('_')
        if not b[-1].isdigit():       #处理task_name中形如“R3_2_”等命名不规则的情况
            del b[-1]
        maxvalue1,r=find_maxvalue(b)
        d.append(r)
        maxvalue.append(maxvalue1)
    return d,max(maxvalue)


def to_graph(a,d):
    for i in range(len(d)):
        if len(d[i])==2:
            start=d[i][1]-1
            end = d[i][0]-1
            a[start,end]=1
        if len(d[i])>2:
            end = d[i][0]-1
            for j in range(1,len(d[i])):
                a[d[i][j]-1,end] = 1
    return a


'''          
def drawDAG(ArrivaMatList):
    G = nx.DiGraph()  
    #nodes = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
    #G.add_nodes_from(nodes)
    Matrix=ArrivaMatList[37]
    for i in range(len(Matrix)):
        for j in range(len(Matrix)):
            if Matrix[i,j]:
                G.add_edge(i+1, j+1)
    #G=G.to_directed()
    pos = nx.spring_layout(G)
    nx.draw(G,pos, with_labels=True, edge_color='b', node_color='g', node_size=1000)
    plt.show()

'''


def opTask(task):
    d,maxvalue=str_to_int(task)
    a=np.mat(np.zeros((maxvalue,maxvalue)))
    a=to_graph(a,d)
    return a

def extractParent(job):
    child=[]
    task=job['task_name'].tolist()
    for i in range(len(task)):
        b=task[i].split(task[i][0])[1].split('_')
        if not b[-1].isdigit():       #处理task_name中形如“R3_2_”等命名不规则的情况
            del b[-1]
        if len(b)>1:
            a={}
            a['task_name']=b[0]
            a['parent']=[b[i] for i in range(1,len(b))]
            child.append(a)
    return child
                
    
def taskToDict(job):
    taskinfo=[]
    #task={}
    for i in range(len(job)):
        task={}
        task['task_name']=job.iloc[i,0]
        task['run_time']=job.iloc[i,6]-job.iloc[i,5]
        taskinfo.append(task)
    return taskinfo

"""
def jobStruct(job):
    for task in job['task_name']:
"""        

def jobToDict(jobs_info):
    DAGinfo=[]
    for job in jobs_info:
        DAG={}
        DAG['job_name']=job.iloc[0,2]
        DAG['task_info']=taskToDict(job)
        DAG['child']=extractParent(job)
        DAGinfo.append(DAG)
    return DAGinfo

class MyEncoder(json.JSONEncoder):
    """
    json默认支持的类型只有dict list tuple str int float bool等
    自定义的类或者date类型，需要自定义jsonEncoder
    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)

def exportJSON(job_json):
    for i in range(len(job_json)):
        filename=job_json[i]['job_name']+'.json'
        with open('./file/'+filename,'w')as f:
            json.dump(job_json[i],f,cls=MyEncoder)
            print("file %s is done..." %filename )

if __name__=="__main__":
    batch_task_filt=batch_task[batch_task['task_type']==1]  #筛选类型1的任务
    batch_task_final=dropOutlier(batch_task_filt)     #清洗异常值
    
    element_job_count=dict(batch_task_final['job_name'].value_counts())
    jobs_info=calJobsInfo(batch_task_final,element_job_count)   #job信息
    job_task_list=extractTask(jobs_info)   
    ArrivaMatList=[opTask(task) for task in job_task_list]  #矩阵形式的job信息
    
    
    job_json=jobToDict(jobs_info)     
    exportJSON(job_json)

d={
  "adag": {
    "-xmlns": "http://pegasus.isi.edu/schema/DAX",
    "-xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "-xsi:schemaLocation": "http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-2.1.xsd",
    "-version": "2.1",
    "-count": "1",
    "-index": "0",
    "-name": "test",
    "-jobCount": "25",
    "-fileCount": "0",
    "-childCount": "20",
    "job": [
      {
        "-id": "ID00000",
        "-namespace": "Montage",
        "-name": "mProjectPP",
        "-version": "1.0",
        "-runtime": "13.39",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "2mass-atlas-ID00000s-jID00000.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4222080"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4167312"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4167312"
          }
        ]
      },
      {
        "-id": "ID00001",
        "-namespace": "Montage",
        "-name": "mProjectPP",
        "-version": "1.0",
        "-runtime": "13.83",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4222080"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4171851"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4171851"
          }
        ]
      },
      {
        "-id": "ID00002",
        "-namespace": "Montage",
        "-name": "mProjectPP",
        "-version": "1.0",
        "-runtime": "13.36",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "2mass-atlas-ID00002s-jID00002.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4222080"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157122"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157122"
          }
        ]
      },
      {
        "-id": "ID00003",
        "-namespace": "Montage",
        "-name": "mProjectPP",
        "-version": "1.0",
        "-runtime": "13.60",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "2mass-atlas-ID00003s-jID00003.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4222080"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4174004"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4174004"
          }
        ]
      },
      {
        "-id": "ID00004",
        "-namespace": "Montage",
        "-name": "mProjectPP",
        "-version": "1.0",
        "-runtime": "13.78",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "2mass-atlas-ID00004s-jID00004.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4222080"
          },
          {
            "-file": "p2mass-atlas-ID00004s-jID00004.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4153521"
          },
          {
            "-file": "p2mass-atlas-ID00004s-jID00004_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4153521"
          }
        ]
      },
      {
        "-id": "ID00005",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.59",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4167312"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4167312"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4171851"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4171851"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "272"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "408404"
          }
        ]
      },
      {
        "-id": "ID00006",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.59",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4185623"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4185623"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4181449"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4181449"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "282"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "314191"
          }
        ]
      },
      {
        "-id": "ID00007",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.88",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4155530"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4155530"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4174004"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4174004"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "287"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "228602"
          }
        ]
      },
      {
        "-id": "ID00008",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.81",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157122"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157122"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4143677"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4143677"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "272"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "176356"
          }
        ]
      },
      {
        "-id": "ID00009",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.49",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4149806"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4149806"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4150602"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4150602"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "262"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "233476"
          }
        ]
      },
      {
        "-id": "ID00010",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.51",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4152397"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4152397"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "267"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "251206"
          }
        ]
      },
      {
        "-id": "ID00011",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.51",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4173072"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4173072"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4170398"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4170398"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "274"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "271248"
          }
        ]
      },
      {
        "-id": "ID00012",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.62",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00004s-jID00004.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4153521"
          },
          {
            "-file": "p2mass-atlas-ID00004s-jID00004_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4153521"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4155664"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4155664"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "271"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "313128"
          }
        ]
      },
      {
        "-id": "ID00013",
        "-namespace": "Montage",
        "-name": "mDiffFit",
        "-version": "1.0",
        "-runtime": "10.37",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "p2mass-atlas-ID00004s-jID00004.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4141389"
          },
          {
            "-file": "p2mass-atlas-ID00004s-jID00004_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4141389"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4163196"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4163196"
          },
          {
            "-file": "fit.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "297"
          },
          {
            "-file": "diff.txt",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "297231"
          }
        ]
      },
      {
        "-id": "ID00014",
        "-namespace": "Montage",
        "-name": "mConcatFit",
        "-version": "1.0",
        "-runtime": "0.72",
        "uses": [
          {
            "-file": "fits_list.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "245"
          },
          {
            "-file": "fit.txt",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "272"
          },
          {
            "-file": "diff.txt",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "408404"
          },
          {
            "-file": "fits.tbl",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "1889"
          }
        ]
      },
      {
        "-id": "ID00015",
        "-namespace": "Montage",
        "-name": "mBgModel",
        "-version": "1.0",
        "-runtime": "1.42",
        "uses": [
          {
            "-file": "pimages.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "837"
          },
          {
            "-file": "fits.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "1889"
          },
          {
            "-file": "corrections.tbl",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "265"
          }
        ]
      },
      {
        "-id": "ID00016",
        "-namespace": "Montage",
        "-name": "mBackground",
        "-version": "1.0",
        "-runtime": "10.39",
        "uses": [
          {
            "-file": "corrections.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "265"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4163084"
          },
          {
            "-file": "p2mass-atlas-ID00000s-jID00000_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4163084"
          },
          {
            "-file": "c2mass-atlas-ID00000s-jID00000_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4163084"
          },
          {
            "-file": "c2mass-atlas-ID00000s-jID00000.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4163084"
          }
        ]
      },
      {
        "-id": "ID00017",
        "-namespace": "Montage",
        "-name": "mBackground",
        "-version": "1.0",
        "-runtime": "10.64",
        "uses": [
          {
            "-file": "corrections.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "265"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4161831"
          },
          {
            "-file": "p2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4161831"
          },
          {
            "-file": "c2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4161831"
          },
          {
            "-file": "c2mass-atlas-ID00001s-jID00001.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4161831"
          }
        ]
      },
      {
        "-id": "ID00018",
        "-namespace": "Montage",
        "-name": "mBackground",
        "-version": "1.0",
        "-runtime": "10.83",
        "uses": [
          {
            "-file": "corrections.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "265"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4182323"
          },
          {
            "-file": "p2mass-atlas-ID00002s-jID00002_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4182323"
          },
          {
            "-file": "c2mass-atlas-ID00002s-jID00002_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4182323"
          },
          {
            "-file": "c2mass-atlas-ID00002s-jID00002.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4182323"
          }
        ]
      },
      {
        "-id": "ID00019",
        "-namespace": "Montage",
        "-name": "mBackground",
        "-version": "1.0",
        "-runtime": "10.93",
        "uses": [
          {
            "-file": "corrections.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "265"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4170858"
          },
          {
            "-file": "p2mass-atlas-ID00003s-jID00003_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4170858"
          },
          {
            "-file": "c2mass-atlas-ID00003s-jID00003.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4170858"
          },
          {
            "-file": "c2mass-atlas-ID00003s-jID00003_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4170858"
          }
        ]
      },
      {
        "-id": "ID00020",
        "-namespace": "Montage",
        "-name": "mBackground",
        "-version": "1.0",
        "-runtime": "10.76",
        "uses": [
          {
            "-file": "corrections.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "265"
          },
          {
            "-file": "p2mass-atlas-ID00004s-jID00004.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157647"
          },
          {
            "-file": "p2mass-atlas-ID00004s-jID00004_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157647"
          },
          {
            "-file": "c2mass-atlas-ID00004s-jID00004.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157647"
          },
          {
            "-file": "c2mass-atlas-ID00004s-jID00004_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157647"
          }
        ]
      },
      {
        "-id": "ID00021",
        "-namespace": "Montage",
        "-name": "mImgTbl",
        "-version": "1.0",
        "-runtime": "1.39",
        "uses": [
          {
            "-file": "cimages.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "837"
          },
          {
            "-file": "c2mass-atlas-ID00000s-jID00000_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4163084"
          },
          {
            "-file": "c2mass-atlas-ID00000s-jID00000.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4163084"
          },
          {
            "-file": "c2mass-atlas-ID00001s-jID00001_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4161831"
          },
          {
            "-file": "c2mass-atlas-ID00001s-jID00001.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4161831"
          },
          {
            "-file": "c2mass-atlas-ID00002s-jID00002_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4182323"
          },
          {
            "-file": "c2mass-atlas-ID00002s-jID00002.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4182323"
          },
          {
            "-file": "c2mass-atlas-ID00003s-jID00003.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4170858"
          },
          {
            "-file": "c2mass-atlas-ID00003s-jID00003_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4170858"
          },
          {
            "-file": "c2mass-atlas-ID00004s-jID00004.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157647"
          },
          {
            "-file": "c2mass-atlas-ID00004s-jID00004_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "4157647"
          },
          {
            "-file": "newcimages.tbl",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "1599"
          }
        ]
      },
      {
        "-id": "ID00022",
        "-namespace": "Montage",
        "-name": "mAdd",
        "-version": "1.0",
        "-runtime": "3.03",
        "uses": [
          {
            "-file": "region.hdr",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "304"
          },
          {
            "-file": "newcimages.tbl",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "1599"
          },
          {
            "-file": "mosaic_ID00022_ID00022.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "46509614"
          },
          {
            "-file": "mosaic_ID00022_ID00022_area.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "46509614"
          }
        ]
      },
      {
        "-id": "ID00023",
        "-namespace": "Montage",
        "-name": "mShrink",
        "-version": "1.0",
        "-runtime": "3.86",
        "uses": [
          {
            "-file": "mosaic_ID00022_ID00022.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "46509614"
          },
          {
            "-file": "mosaic_ID00022_ID00022_area.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "46509614"
          },
          {
            "-file": "shrunken_ID00023_ID00023.fits",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "1861129"
          }
        ]
      },
      {
        "-id": "ID00024",
        "-namespace": "Montage",
        "-name": "mJPEG",
        "-version": "1.0",
        "-runtime": "0.45",
        "uses": [
          {
            "-file": "shrunken_ID00023_ID00023.fits",
            "-link": "input",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "1861129"
          },
          {
            "-file": "shrunken_ID00023_ID00023.jpg",
            "-link": "output",
            "-register": "true",
            "-transfer": "true",
            "-optional": "false",
            "-type": "data",
            "-size": "204856"
          }
        ]
      }
    ],
    "child": [
      {
        "-ref": "ID00005",
        "parent": [
          { "-ref": "ID00001" },
          { "-ref": "ID00000" }
        ]
      },
      {
        "-ref": "ID00006",
        "parent": [
          { "-ref": "ID00001" },
          { "-ref": "ID00000" }
        ]
      },
      {
        "-ref": "ID00007",
        "parent": [
          { "-ref": "ID00001" },
          { "-ref": "ID00003" }
        ]
      },
      {
        "-ref": "ID00008",
        "parent": [
          { "-ref": "ID00002" },
          { "-ref": "ID00000" }
        ]
      },
      {
        "-ref": "ID00009",
        "parent": [
          { "-ref": "ID00002" },
          { "-ref": "ID00001" }
        ]
      },
      {
        "-ref": "ID00010",
        "parent": { "-ref": "ID00002" }
      },
      {
        "-ref": "ID00011",
        "parent": [
          { "-ref": "ID00001" },
          { "-ref": "ID00003" }
        ]
      },
      {
        "-ref": "ID00012",
        "parent": [
          { "-ref": "ID00001" },
          { "-ref": "ID00004" }
        ]
      },
      {
        "-ref": "ID00013",
        "parent": [
          { "-ref": "ID00004" },
          { "-ref": "ID00003" }
        ]
      },
      {
        "-ref": "ID00014",
        "parent": [
          { "-ref": "ID00012" },
          { "-ref": "ID00013" },
          { "-ref": "ID00010" },
          { "-ref": "ID00011" },
          { "-ref": "ID00006" },
          { "-ref": "ID00005" },
          { "-ref": "ID00009" },
          { "-ref": "ID00008" },
          { "-ref": "ID00007" }
        ]
      },
      {
        "-ref": "ID00015",
        "parent": { "-ref": "ID00014" }
      },
      {
        "-ref": "ID00016",
        "parent": [
          { "-ref": "ID00000" },
          { "-ref": "ID00015" }
        ]
      },
      {
        "-ref": "ID00017",
        "parent": [
          { "-ref": "ID00001" },
          { "-ref": "ID00015" }
        ]
      },
      {
        "-ref": "ID00018",
        "parent": [
          { "-ref": "ID00002" },
          { "-ref": "ID00015" }
        ]
      },
      {
        "-ref": "ID00019",
        "parent": [
          { "-ref": "ID00003" },
          { "-ref": "ID00015" }
        ]
      },
      {
        "-ref": "ID00020",
        "parent": [
          { "-ref": "ID00004" },
          { "-ref": "ID00015" }
        ]
      },
      {
        "-ref": "ID00021",
        "parent": [
          { "-ref": "ID00020" },
          { "-ref": "ID00016" },
          { "-ref": "ID00017" },
          { "-ref": "ID00018" },
          { "-ref": "ID00019" }
        ]
      },
      {
        "-ref": "ID00022",
        "parent": { "-ref": "ID00021" }
      },
      {
        "-ref": "ID00023",
        "parent": { "-ref": "ID00022" }
      },
      {
        "-ref": "ID00024",
        "parent": { "-ref": "ID00023" }
      }
    ]
  }
}


