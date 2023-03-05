import numpy as np
import collections 
from time import time

threshold_factor = 0.98 # supported threshold = number of basket * threshold factor
sample_size = 0.1 # sampling ratio in random sampling and SON

def data_load(dataset):
    with open(dataset,'r') as f:
        data_list = []
        np_data_list = [] 
        for line in f.readlines(): 
            item_list = line.split()
            np_data_list.append(np.asarray(item_list, dtype = str))
            data_list += item_list
        
        np_set = np.unique(np.asarray(data_list, dtype = str)).tolist() #Set of label

        return np_data_list, np_set

def filter(data_list, label_list, support_threshold):
    
    count_data = []
    for label in label_list: 
        try:
            temp_label = label[0].split() # split the label if it is not single item
        except:
            temp_label = label
        count = 0 
        for data in data_list:
            if set(temp_label).issubset(data.tolist()): #check if the set of label is a subset of a basket
                count += 1
        temp = [label, count]

        count_data.append(temp)

    freq_item_list = []
    for item in count_data:
        if item[1] > support_threshold: #check if it is above the support threshold
            freq_item_list.append(item[0])
    return freq_item_list

def construt(freq_item_list):
    freq_pair_list = []

    for index_i, i in enumerate(freq_item_list):
        for index_j, j in enumerate(freq_item_list):
                        
            if index_i < index_j:
                try:

                    temp_i = i[0].split()
                    temp_j = j[0].split()
 
                except:
                    temp_i = i
                    temp_j = j

                temp = np.unique([temp_i]+[temp_j])

                if temp.shape[0] == len(temp_i)+1:
                    sep = ' '
                    temp = sep.join(temp) #create the label from list of item
                    freq_pair_list.append([temp])
                
    return np.unique(freq_pair_list, axis =0)

def A_priori(np_data_list, np_set):
    
    number_of_basket = len(np_data_list)
    N = number_of_basket
    print("Number of Basket:")
    print(N)
    s = np.round(N * threshold_factor)
    print("Supported threshold:")
    print(s)
    final_freq_item_list = []
    
    data = np_data_list
    #label = np_set
    label = []
    for i in np_set:
        label.append(np.asarray([i]))
    while True:
        
        #print(s)
        freq_item_list = filter(data, label, support_threshold = s)
        #print(freq_item_list)
        freq_pair_list = construt(freq_item_list)
        label = freq_pair_list
        #print(freq_pair_list)
        final_freq_item_list += freq_item_list

        #N = s
        if len(freq_pair_list) == 0:
            break
    return np.unique(final_freq_item_list, axis = 0)

def random_sample(dataset, p = 0.01):
    with open(dataset,'r') as f:
        data_list = []
        np_data_list = [] 
        for line in f.readlines(): 
            item_list = line.split()
            #print(item_list)
            if np.random.random() < p: # if the random number drawn is smaller the p, then include the data point as sample
                np_data_list.append(np.asarray(item_list, dtype = str))
                data_list += item_list
        
        np_set = np.unique(np.asarray(data_list, dtype = str)).tolist() #Set of label

        return np_data_list, np_set

def son_algorithm(dataset):
    
    # 1st pass
    freq_item_list = [] 
    average_basket_size = 0
    for i in range(10):
        
        np_data_list_RS, np_set_RS = random_sample(dataset, p = sample_size)
        N = len(np_data_list_RS)
        freq_item_list_RS = A_priori(np_data_list_RS, np_set_RS)
        freq_item_list += freq_item_list_RS.tolist()
        average_basket_size +=N
    average_basket_size /= 10

    print(f'average_basket_size:{average_basket_size}')
    # 2nd pass
    np_data_list, _ = data_load(dataset)
    number_of_basket = len(np_data_list)
    N = number_of_basket

    label = []
    for i in np.unique(freq_item_list):
        label.append(np.asarray([i]))

    #print(label)
    s = np.round(N * threshold_factor)
    real_freq = filter(np_data_list, label, support_threshold = s)
    #print()
    return np.unique(real_freq, axis = 0)
if __name__ == "__main__":
    dataset = 'chess.dat'
    #dataset = 'pumsb.dat'
    #dataset = 'T10I4D100K.dat'
    print(f'threshold factor:{threshold_factor}')
    print(f'sample size factor:{sample_size}')
    
    #uncomment the below block for running A-Priori

    #print("A_priori:")
    #t1 = time()
    #np_data_list, np_set = data_load(dataset)
    #freq_item_list_base = A_priori(np_data_list, np_set)
    #t2 = time()
    #print("frequent item list:")
    #print(freq_item_list_base)
    #print("Number of freq itme:")
    #print(len(freq_item_list_base))
    #print(f'Runtime:{t2-t1}')

    print("Random Sampling:")
    t3 = time()
    np_data_list_RS, np_set_RS = random_sample(dataset, p = sample_size)
    freq_item_list_RS = A_priori(np_data_list_RS, np_set_RS)
    t4 = time()
    print("frequent item list:")
    print(freq_item_list_RS)
    print("Number of freq itme:")
    print(len(freq_item_list_RS))
    print(f'Runtime:{t4-t3}')

    #print(np_data_list_RS)
    #print(len(np_data_list_RS))
    
    #print(np_set)
    
    print("son")
    t5 = time()
    freq_item_list_son = son_algorithm(dataset)
    t6 = time()
    print("frequent item list:")
    print(freq_item_list_son)
    print("Number of freq itme:")
    print(len(freq_item_list_son))
    print(f'Runtime:{t6-t5}')



     
