import numpy as np
import sys
m = int(sys.argv[1])
p = int(sys.argv[2])
q = int(sys.argv[3])

A = np.random.rand(m,p)
B = np.random.rand(p,q)
C = np.dot(A,B)

np.savetxt('A.txt',A)
np.savetxt('B.txt',B)
np.savetxt('CTest.txt',C)