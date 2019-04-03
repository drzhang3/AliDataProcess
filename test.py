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


