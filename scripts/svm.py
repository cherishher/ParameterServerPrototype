import os
num_of_nodes = 1
cmd = ""
print cmd
port = 12710
master_port = 32350
for i in range(0, num_of_nodes):
  cmd += "ssh 1155046948@proj" + str(10 - i) + " "
  cmd += "/data/opt/tmp/1155046948/csci5570/build/./SVM " + \
    str(10 - i) + " " + str(num_of_nodes) + " " + \
    str(port) + " " + str(master_port + i) + "&"
    print (cmd)
    os.system(cmd)
    cmd = ""
while(True):
  pass
# print cmd
