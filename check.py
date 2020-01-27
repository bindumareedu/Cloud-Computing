import subprocess

subprocess.run(["java","-jar","mr.jar","/home/bindu/map_reduce_assignment/mapper.jar","-r","/home/bindu/map_reduce_assignment/reducer.jar","-i","/home/bindu/map_reduce_assignment/story.txt","-o","/home/bindu/map_reduce_assignment/"])