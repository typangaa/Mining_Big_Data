import matplotlib.pyplot as plt
from sklearn import datasets
import numpy as np

def distant(a,b): # calculate the distance between 2 points
    return np.linalg.norm(a-b)

def initial_centroids(input_data, k): # randomly pick k points as centroids
    number_of_row = input_data.shape[0]
    row_indice = np.random.choice(number_of_row, size = k, replace = False)

    return input_data[row_indice, :]

def assign_to_centroids(input_data, centroids): # assign the datapoints to the nearest centriods

    number_of_row = input_data.shape[0]
    centroid_assignment = np.zeros(number_of_row)

    for index_i, i in enumerate(input_data):
        temp_distant = 100000 
        for index_c, c in enumerate(centroids):
            if distant(i,c) <=  temp_distant: # find the nearest centriods
                temp_distant = distant(i,c)
                centroid_assignment[index_i] = index_c
    return centroid_assignment

def new_centroids(input_data, centroid_assignment): # calculate the new centriods of each clusters

    for index_c, c in enumerate(centroids):
        cluster_index = np.where(centroid_assignment == index_c)[0]
        cluster_list = input_data[cluster_index, :]
        centroids[index_c,:] = np.mean(cluster_list, axis = 0) # new centriod is the mean of the cluster

    return centroids

def plot_2d(X, y,centroids, num = 1):
    x_min, x_max = X[:, 0].min() - .5, X[:, 0].max() + .5
    y_min, y_max = X[:, 1].min() - .5, X[:, 1].max() + .5
    
    plt.figure(num, figsize=(8, 6))
    plt.clf()

    # Plot the training points
    plt.scatter(X[:, 0], X[:, 1], c=y, cmap=plt.cm.Set1,
                edgecolor='k')
    c = [0,1,2]
    plt.scatter(centroids[:, 0], centroids[:, 1],c= c, s = 100, marker ='x', cmap=plt.cm.Set1)

    plt.xlabel('Sepal length')
    plt.ylabel('Sepal width')

    plt.xlim(x_min, x_max)
    plt.ylim(y_min, y_max)
    plt.xticks()
    plt.yticks()

    plt.show()

if __name__ == "__main__":
    pass
    iris = datasets.load_iris()

    X = iris.data  
    y = iris.target

    k = 3 

    centroids = initial_centroids(X, k)
    #print(centroids)
    old_centroids = np.zeros(centroids.shape)
    
    while distant(centroids, old_centroids) >0.01:
        

        centroid_assignment = assign_to_centroids(X,centroids)
        print("Assignment")
        print(centroid_assignment)
        old_centroids = centroids.copy()
        centroids = new_centroids(X,centroid_assignment)
        print("old")
        print(old_centroids)
        print("current")
        print(centroids)

    print("True label:")
    print(y)

    #plot_2d(X,y,num = 1)
    plot_2d(X,centroid_assignment,centroids,num = 2)
   
    