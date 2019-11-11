from .mapreducejob import MapReduceJob

def run_mapreducejob():
    mapreducejob = MapReduceJob()
    mapreducejob.mapreduce()

if __name__ == "__main__":
    #TODO: Parse arguments
    #TODO: Choose program to run
    run_mapreducejob()
