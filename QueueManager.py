import os
from pathlib import Path 
import shutil
import json

def read_json(path):
    # helper function to read json contents from filepath
    with open(path, "r") as fp:
        return json.loads(fp.read())

def write_json(path, data):
    # helper function to write json data to filepath
    with open(path, "w") as fp:
        fp.write(json.dumps(data))

class QueueManagerError(Exception):
    def __init__(self, message="", header="Queue Manager Error:"):
        self.message = message
        self.header = header
        super().__init__(self.header.strip + self.message)

class QueueManager:

    def __init__(self, base):
        # base directory, where all other queues live
        self.base = Path(base).absolute()
        # path to failed jobs directory
        self.failed = self.base / "failed"
        # path to nested directory of inprocess jobs for each device
        self.inprocess = self.base / "inprocess"
        # path to tmp dir which holds configs and output/error logs for jobs
        self.tmp = self.base / "tmp"
        # path to nested dir for each queue containing waiting jobs
        self.waiting = self.base / "waiting"

    def queue_range(self, src):
        # get the minimum and maximum numbers from a queue
        items = [int(x) for x in os.listdir(src)]
        if len(items) == 0:
            return (-1,-1)
        return (min(items), max(items))

    def list_waiting_queues(self):
        # return a list of the names of our waiting queues
        return list(os.listdir(self.waiting))

    def waiting_queue_lengths(self):
        # return a dictionary mapping the name of each waiting queue to
        # the number of items within it - a better version of list_waiting_queues
        lens = {}
        for wq in self.list_waiting_queues():
            lens[wq] = len(os.listdir(self.waiting / wq))
        return lens

    def read_waiting_queue_item(self, src, src_num):
        # return the json contents of an item from a waiting queue
        return read_json(self.waiting / src / str(src_num))

    def read_waiting_queue_items(self, src):
        # return a list of the json contents of all the items from a waiting queue
        output = []
        if not os.path.isdir(self.waiting / src):
            return output
        items = [int(x) for x in os.listdir(self.waiting / src)]
        for item in items:
            output.append(read_json(self.waiting / src / str(item)))
        return output
    
    def swap_waiting_queue_items(self, a, a_num, b, b_num):
        # swap queue items
        # a: name of first queue in waiting, ex "main", "example_pipeline", etc
        # a_num: number of item to swap from a
        # b: name of second queue in waiting, can be the same or different as a
        # b_num: number of item to swap from b
        a_path = self.waiting / Path(a) / str(a_num)
        a_data = read_json(a_path)
        b_path = self.waiting / Path(b) / str(b_num)
        b_data = read_json(b_path)
        write_json(b_path, a_data)
        write_json(a_path, b_data)
        return True

    def move_waiting_queue_item(self, src, src_num, dst, dst_num):
        # move queue item from waiting/src/src_num to waiting/dst/dst_num
        # src: name of source queue in waiting, ex "main", "example_pipeline", etc
        # src_num: number of item to move from src
        # dst: name of destination queue in waiting, ex "main", "example_pipeline", etc
        # dst_num: number of item to move to in dst
        src_path = self.waiting / Path(src) / str(src_num)
        dst_path = self.waiting / Path(dst) / str(dst_num)
        shutil.move(src_path, dst_path)
        return True

    def send_waiting_to_front(self, src, src_num, dst=None):
        # move a waiting job from waiting/src/src_num to waiting/dst/{front}
        # src: name of queue in waiting, ex "main", "example_pipeline", etc
        # src_num: number of item to move from src
        if dst is None:
            dst = src
        dst_min, dst_max = self.queue_range(self.waiting / dst)
        if dst_min == -1 and dst_max == -1:
            dst_min = 1
        if dst_min == 0 and dst_min != 0:
            raise QueueManagerError("Cannot move to front now, wait until first job finishes")
        if dst_min == 0 and dst_max == 0:
            return self.send_waiting_to_back(src, src_num, dst)
        dst_num = dst_min - 1
        if src == dst and src_num == dst_num:
            raise QueueManagerError("Item is already index {dst_num} in {dst}")
        self.move_waiting_queue_item(src, src_num, dst, dst_num)
        return True

    def send_waiting_to_back(self, src, src_num, dst=None):
        # move a waiting job from waiting/src/src_num to waiting/dst/{back}
        if dst is None:
            dst = src
        _, dst_max = self.queue_range(self.waiting / dst)
        dst_num = dst_max + 1
        self.move_waiting_queue_item(src, src_num, dst, dst_num)
        return True
        
    def delete_waiting_queue_item(self, src, src_num):
        # remove a waiting queue item 
        os.remove(self.waiting / src/ str(src_num))
        return True

if __name__ == "__main__":
    qm = QueueManager("./example_queue_structure")
    print(qm.read_waiting_queue_item("main", 23))