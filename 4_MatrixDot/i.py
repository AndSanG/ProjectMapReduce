import numpy as np
'''
A = np.loadtxt('A.txt')
B = np.loadtxt('B.txt')
C = A@B
print(C)
'''
A = np.random.rand(4,5)

B = np.random.rand(5,2)

np.savetxt('Asmall.txt',A)
np.savetxt('Bsmall.txt',B)

C = np.dot(A,B)
np.savetxt('Csmall.txt',C)

