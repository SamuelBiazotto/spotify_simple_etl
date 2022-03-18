from extract.extract import Extract
from load.load import Load
from transform.transform import Transform

class Pipeline():

    def __init__(self) -> None:
        self.start_pipeline()

    def start_pipeline(self):
        print("Pipeline Started \n")
        
        extract = Extract()
        extract.__run__()

        transform = Transform()
        transform.__run__()

        load = Load()
        load.__run__()

        print("\nPipeline Finished")

Pipeline()