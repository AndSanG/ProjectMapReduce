import numpy as np
'''
A = np.loadtxt('A.txt')
B = np.loadtxt('B.txt')
C = A@B
print(C)
'''
A = np.random.rand(1000,50)

B = np.random.rand(50,2000)

np.savetxt('A.txt',A)
np.savetxt('B.txt',B)

C = np.dot(A,B)
np.savetxt('CTest.txt',C)


