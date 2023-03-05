import numpy as np
from  scipy import sparse
import time

dataset = "PageRank_toy_examples/graph_1.txt"
DEFAULT_DATASET = "web-Google.txt"

def data_to_csr(dataset):
    with open(dataset,'r') as f:
        row = []
        col = []
        global node #Define as global variable 
        node = 0
        global edge #Define as global variable 
        edge = 0 
        global dict_set2index #Define as global variable 
        dict_set2index = {}
        global dict_index2set #Define as global variable 
        dict_index2set = {}

        if dataset == DEFAULT_DATASET: #For google dataset
            startline = 4
            sep = '\t'
        else: #For test case
            startline = 0
            sep = ','
        for line in f.readlines()[startline:]: 
            origin, destiny = (int(x) for x in line.split(sep))
            row.append(destiny)
            col.append(origin)
            edge += 1
        np_list = np.asarray(row+col)
        global np_set
        np_set = np.unique(np_list) #Set of node label
        np_index = np.arange(np_set.shape[0]) #List of index

        
        for A, B in zip(np_set, np_index): # Create dictionary for node label and index in both direction
            dict_set2index[A] = B
            dict_index2set[B] = A

        row_index = []
        col_index = []
        for i in row:
            row_index.append(dict_set2index[i])  #Convert the label to index
        for j in col:
            col_index.append(dict_set2index[j])  #Convert the label to index

        node = np_set.shape[0] 
        
        print(f'number of edge:{edge}')
        print(f'number of node:{node}')
        
        return sparse.csr_matrix(([True]*edge,(row_index,col_index)),shape=(node,node))


def PageRank(adjacency_matrix, beta=0.85, epsilon=0.0001):
    
    current_rank = 1/node
    flag = True
    while flag:        
        with np.errstate(divide='ignore'): # Ignore division by 0 
            deg_out = adjacency_matrix.sum(axis=0).T # out degree value 
            temp = (current_rank*beta/deg_out) # r*B/d_out
            new_ranks = adjacency_matrix.dot(temp)  

        #Leaked PageRank
        new_ranks += (1-new_ranks.sum())/node
        #Stop condition
        if np.linalg.norm(current_rank-new_ranks,ord=1)<=epsilon:
            flag = False        
        current_rank = new_ranks
    return current_rank

def get_max(arr, num_max = 10): #Get the top number of max value and index in array
    ind = np.argpartition(arr, -num_max)[-num_max:]
    sort_max_arr = np.flip(arr[ind[np.argsort(arr[ind])]])
    sort_max_index = np.flip(ind[np.argsort(arr[ind])])
    
    return sort_max_arr, sort_max_index

t1 = time.time()
csr_m = data_to_csr(DEFAULT_DATASET)
t2 = time.time()
rank_matrix = PageRank(csr_m)
t3 = time.time()

import sys


print ("The size in memory of the adjacency matrix is {0} MB".format(
    (sys.getsizeof(csr_m.shape)+
    csr_m.data.nbytes+
    csr_m.indices.nbytes+
    csr_m.indptr.nbytes)/(1024.0**2)))

print(f"Runtime of data_to_csr:{np.round(t2-t1,2)}s")
print(f"Runtime of PageRank:{np.round(t3-t2,2)}s")

rank_numpy = np.asarray(rank_matrix).reshape(-1)
arr_max , arr_max_index = get_max(rank_numpy,10)

print("Top rank value:")
print(arr_max)
print("Top rank index")
print(arr_max_index)

original_label = [] 
for index in arr_max_index:
    original_label.append(dict_index2set[index])
print("original label:")
print(original_label)

np.savetxt('PageRank.txt',np.vstack([np_set,rank_numpy]).T, fmt='%6d %s')
    