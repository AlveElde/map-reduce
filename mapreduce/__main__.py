from .mapreducejob import MapReduceJob

def run_mapreducejob(mappers, reducers):
    mapreducejob = MapReduceJob()
    mapreducejob.mapreduce(mappers, reducers)

if __name__ == "__main__":
    #TODO: Parse arguments
    #TODO: Choose program to run
    run_mapreducejob(2, 2)
