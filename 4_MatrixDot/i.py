import numpy as np
'''
A = np.loadtxt('A.txt')
B = np.loadtxt('B.txt')
C = A@B
print(C)
'''
A = np.random.rand(10,5)

B = np.random.rand(5,20)

np.savetxt('Asmall.txt',A)
np.savetxt('Bsmall.txt',B)

C = np.dot(A,B)
np.savetxt('CsmallTest.txt',C)


